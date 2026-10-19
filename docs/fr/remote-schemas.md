# Schémas distants {: #remote-schemas }

Une source de schéma distant relie une API externe, qu'il s'agisse de GraphQL (GitHub compris), de gRPC ou de REST (OpenAPI), au modèle Provisa. Ajouter la source n'enregistre aucune table. La source propose des tables, et un intendant enregistre chacune de celles qu'il veut dans le sélecteur Register Table ; cet enregistrement est l'étape de curation. Une table enregistrée est une table Provisa à part entière : toute règle de gouvernance, toute interface de requête et toute couche de sécurité s'y appliquent automatiquement. Le service distant ne voit jamais les règles de gouvernance de Provisa.

---

## Trois types de source {: #three-source-types }

### Schéma distant GraphQL {: #graphql-remote-schema }

**Comment ajouter la source.** Envoyer une requête POST à `/admin/sources/graphql-remote` avec l'URL du endpoint, un namespace et une authentification facultative. Provisa déclenche une requête d'introspection `__schema` standard sur le endpoint distant afin de confirmer le endpoint et l'identifiant.

Ajouter la source n'enregistre ni table ni command. Tout type de source distante répond à l'ajout et à l'actualisation avec les mêmes compteurs : `tables` (tables enregistrées ou mises à jour ; 0 à l'ajout), `available_tables` (tables proposées), `mutations` (toujours 0) et `available_mutations` (commands proposées).

**Enregistrer des tables.** Ouvrir Tables, puis Register Table, choisir la source et le schéma `graphql`, puis sélectionner les tables et colonnes souhaitées. Via l'API GraphQL d'administration : `availableTables(sourceId, schemaName)` liste les tables proposées, `availableColumns` liste les colonnes d'une table et `registerTable(input: TableInput)` en enregistre une avec les colonnes choisies. Une table enregistrée est alors gouvernée.

La manière dont une table enregistrée est lue (champ racine, chemin des lignes, arguments obligatoires, arguments de pagination) est stockée dans `sources.mapping["tables"]`, de sorte qu'un processus redémarré la lit sans interroger le distant sur son schéma.

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

Options d'authentification : `none`, `bearer` (en-tête Authorization), `basic` (nom d'utilisateur:mot de passe en Base64).

**Surcharges de champ.** `field_overrides` est une correspondance `{fieldName: "query" | "mutation"}` appliquée après l'introspection. Elle est prioritaire sur la classification structurelle. Seuls les champs de type query peuvent être reclassés en mutation ; les champs de type mutation n'ont pas de chemin de surcharge en GraphQL.

**Relations au moment de l'enregistrement.** `relationships` déclare des chemins de jointure clé étrangère/clé primaire entre tables au moment de l'enregistrement. Elles sont stockées comme des relations déclarées manuellement (sans indicateur `remote_managed`). Lors d'une actualisation, les relations détectées automatiquement (celles avec `remote_managed: True`) sont réexécutées et peuvent changer ; les relations déclarées manuellement ne sont pas modifiées.

**Ce que la source propose.** Tout champ du type `Query` distant qui renvoie un objet ou une liste d'objets est proposé comme table, de même que chaque connexion Relay sous un champ à objet unique (voir ci-dessous). Enregistrer une table proposée en fait une table. Chaque champ du type `Mutation` distant est une commande proposée, comptée dans `available_mutations` ; ajouter la source n'en enregistre aucune. Enregistrez celles que vous voulez comme commandes ; voir [Opération d'écriture d'une source distante](commands.md#a-remote-sources-write-operation).

**Nommage des tables.** Les tables sont nommées `{namespace}__{field_name}`. Avec le namespace `petstore` et un champ de requête `pets` : le nom de la table est `petstore__pets`.

**Connexions Relay.** De nombreuses API renvoient les listes sous forme de connexions Relay : un objet avec `nodes` (ou `edges { node }`) à côté de `pageInfo`. Provisa fait correspondre une connexion à une table de ses nœuds et la lit page par page.

- Un champ racine qui renvoie une connexion (`securityAdvisories`) devient une table de ses nœuds.
- Une connexion sur l'objet unique renvoyé par un champ racine devient sa propre table. La table reprend les arguments obligatoires du champ racine. Avec `repository(owner, name)` et une connexion `issues` sur `Repository`, la table est `repositoryIssues`, de nom SQL `gh__repository_issues` sous le namespace `gh`. Filtrez-la via les colonnes `_nf_owner` et `_nf_name` : `WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`.
- Une connexion n'est jamais une colonne. Sinon, une ligne porterait une lecture que le distant calcule par ligne, pour chaque connexion que son type possède.
- Une connexion n'est une table que si son champ accepte `first` et `after`, afin de pouvoir être lue page par page. Une connexion qui exige un argument qui lui est propre n'est pas une table. Il en va de même d'une connexion d'une union, ou de toute connexion sous un champ racine qui renvoie une liste.

**Correspondance des types.** Les champs scalaires sont directement mis en correspondance avec les types Provisa. Les champs OBJECT se répartissent en deux cas selon que le type cible est gouverné ou non (voir « Tables gouvernées » ci-dessous).

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

**Tables gouvernées.** Un type GQL est gouverné lorsqu'il apparaît comme champ racine de `Query` dans le schéma distant. `_collect_queryable_types` recense ces types lors de l'enregistrement, en privilégiant les champs sans argument obligatoire afin qu'ils puissent être récupérés en masse en tant que cibles de jointure.

Lorsqu'une colonne de type OBJECT sur une table gouvernée pointe vers un autre type gouverné, cette colonne est soumise à trois règles simultanément :

1. **Exclue de la récupération GQL** — le champ n'est pas demandé lors de la récupération des lignes de la table parente.
2. **Exclue du SDL** — le champ n'apparaît pas sur le type parent dans le schéma généré.
3. **Accessible uniquement via une relation déclarée** — un data steward doit enregistrer une jointure entre les deux tables gouvernées matérialisées. En l'absence de cette relation, le champ est simplement absent ; il n'y a pas de repli sous forme de blob.

Les types OBJECT qui ne sont PAS accessibles en tant que champs racine de Query (types inline tels que `ContactInfo` ou `Address`) suivent des règles différentes : ils sont récupérés sous forme de colonnes blob `jsonb` et apparaissent dans le SDL comme des champs d'objet imbriqué. Les sous-champs sont accessibles via une extraction `->>` en SQL.

**Les champs qui exigent un argument ne sont pas des colonnes.** Un champ avec un argument obligatoire ne peut pas être sélectionné tel quel ; il est donc écarté des colonnes de la table et des sélections imbriquées.

**Arguments obligatoires.** Lorsqu'un champ de requête racine possède des arguments non nuls sans valeur par défaut, ceux-ci deviennent des colonnes `native_filter_type: query_param` sur la table (préfixées `_nf_` au moment de l'injection). L'exécuteur les transmet en tant que variables GraphQL.

**Relations détectées automatiquement.** Provisa examine les colonnes de type OBJECT de chaque table enregistrée. Quand le type GQL qu'une colonne référence est aussi une table enregistrée de la même source, et que la colonne qui porte la relation fait partie des colonnes enregistrées, la relation est stockée. Une table pas encore enregistrée n'en reçoit aucune. Les relations many-to-one déduisent leurs colonnes source et cible des conventions de nommage (`breedName` sur le type source → `name` sur le type cible `Breed`). Les champs one-to-many (LIST) produisent des relations aux références de colonnes vides, car la FK se trouve côté cible.

**Mutations.** Un champ de mutation est enregistré, un à un, comme commande de genre `source_operation`. Ses arguments sont chacun typés `json` et transmis au service distant comme variables typées ; la réponse est le JSON que renvoie le service distant, sans `return_schema`. Voir [Opération d'écriture d'une source distante](commands.md#a-remote-sources-write-operation).

**Actualisation.** Envoyer une requête POST à `/admin/sources/graphql-remote/{id}/refresh`. Ré-introspecte le schéma distant et met à jour, par rapport à lui, les tables déjà enregistrées. Elle n'ajoute ni table ni colonne : une table ou une colonne que le schéma a gagnée reste proposée, et une colonne que le schéma a perdue est supprimée. Les règles de gouvernance existantes (RLS, masquage) sont préservées.

**Limitations.**

- Les champs de requête racine de type scalaire et ENUM (dont le type de retour n'est pas OBJECT) deviennent des fonctions suivies, et non des tables virtuelles. Leur `return_schema` est une colonne unique `value` du type scalaire correspondant.
- L'imbrication d'objets est résolue au moment de l'enregistrement jusqu'à `graphql_remote.max_object_depth` (par défaut : 5). La sélection de la récupération distante et les métadonnées des sous-champs sont toutes deux construites jusqu'à cette profondeur ; les champs au-delà de la limite ne sont pas récupérés et ne sont pas disponibles pour l'extraction SQL. Un type n'est visité qu'une fois le long d'un même chemin : un champ dont le type figure déjà sur le chemin descendant est écarté, de sorte qu'un schéma dont les types se réfèrent les uns aux autres est parcouru une fois par type, et non une fois par niveau de profondeur.
- Les champs OBJECT imbriqués de type LIST (p. ex. `breed.awards: [Award]`) sont inclus dans la sélection de récupération jusqu'à `graphql_remote.max_list_depth` niveaux d'imbrication (par défaut : 2). Dans cette limite, la liste est récupérée sous forme de tableau `jsonb` dans la colonne parente. Lorsque le champ de liste déclare un argument `first` (Relay, PostGraphile, pg_graphql) ou un argument `limit` (Hasura), la sélection le transmet sous la forme `first: N` ou `limit: N`, où N vaut `graphql_remote.max_list_items` (par défaut : 100). Un champ de liste qui ne déclare ni l'un ni l'autre ne reçoit aucun argument, car un distant rejette un argument que le champ ne déclare pas. Au-delà de `max_list_depth`, le champ LIST est entièrement exclu afin d'éviter une expansion illimitée des données. En SQL, le tableau est accessible via `json_array_elements(column_name)` ou par extraction d'index avec `->>`. Si le type d'élément de la liste possède sa propre requête racine, enregistrez-le plutôt comme table distincte et créez une relation. Le chemin de jointure est plus efficace et contourne le blob.
- Pour les requêtes SQL, les colonnes de type OBJECT non gouvernées sont récupérées intégralement depuis la source distante (tous les sous-champs jusqu'à la profondeur configurée) et mises en cache sous forme de `jsonb`. L'accès aux sous-champs en SQL est géré via une extraction `->>` sur le blob ; la requête distante n'est pas restreinte aux seuls champs sélectionnés par la requête SQL. Lorsque le type d'élément de la liste n'a pas de requête racine et que la représentation en blob est insuffisante, il convient d'écrire directement la requête en SDL GraphQL. Provisa reproduit fidèlement la sélection de champs GQL, de sorte que la source distante reçoit exactement les champs demandés.
- Si le serveur distant rejette un champ de type OBJECT parce qu'il exige une sélection de sous-champs (ce qui ne devrait pas se produire lorsque `gql_selection` est disponible), l'exécuteur retente une fois en retirant ces champs afin que les colonnes scalaires soient tout de même renvoyées. Cela s'applique aux tables lues depuis un champ racine. Une table de connexion n'emprunte pas ce chemin.

**Lectures paginées.** Une table de connexion est lue par curseur. Chaque page demande `first: N, after: $pageCursor` avec `pageInfo { hasNextPage endCursor }`, et la lecture suit `endCursor` jusqu'à ce que le distant indique qu'il n'y a plus de page suivante.

| Paramètre | Valeur par défaut | Effet |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | Lignes par page. |
| `graphql_remote.max_rows` | `10000` | Le plus grand nombre de lignes qu'une lecture d'une table de connexion prend. Une lecture qui l'atteint s'arrête et consigne un avertissement. |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

Deux réponses amènent l'exécuteur à réessayer :

- **Page trop lourde.** Lorsque le distant répond 502 ou 504, la même page est redemandée à la moitié de sa taille, jusqu'à une ligne.
- **Limite de débit avec délai d'attente.** Lorsque le distant répond 403 ou 429 avec un `Retry-After` de 120 secondes ou moins, l'exécuteur attend ce délai puis renvoie la requête, jusqu'à trois tentatives. Un refus sans `Retry-After`, ou demandant une attente plus longue, est levé comme une erreur. Cela s'applique à toute lecture, de connexion ou non.

Toute autre erreur dans la réponse fait échouer la lecture, sauf si le type de source en décide autrement (voir GitHub ci-dessous). Une connexion dont le parent est revenu nul n'a aucune ligne.

---

### GitHub

GitHub est un type de source ordinaire. Son API est GraphQL ; ses tables se comportent donc comme décrit ci-dessus, y compris les tables de connexion telles que `gh__repository_issues`.

**Ajouter la source.**

1. Ouvrir Sources et ajouter une source de type **GitHub**.
2. Saisir un jeton d'accès GitHub. Saisir éventuellement un namespace, le préfixe des noms de table ; la valeur par défaut est `gh`.
3. Enregistrer. Provisa vérifie le jeton auprès de GitHub. Un jeton que GitHub rejette fait échouer l'ajout avec le message de GitHub.

Ajouter la source n'enregistre aucune table.

**Enregistrer des tables.** Ouvrir Tables, puis Register Table. Choisir la source GitHub, choisir le schéma `graphql`, puis choisir les tables souhaitées. Toute table que GitHub propose est listée ; l'enregistrement est votre choix de ce qu'il faut exposer.

**Portées (scopes) du jeton.** Lorsque vous enregistrez une table, Provisa la vérifie une fois auprès de GitHub avec votre jeton.

- Un champ que les portées du jeton ne couvrent pas est écarté de la table. Le résultat nomme chaque champ écarté : `Left out, because the source's credential may not read them: projectsV2`.
- Une table que le jeton ne peut pas lire du tout est refusée, avec la raison de GitHub : `GitHub does not let this source's credential read gh__repository_issues: ...`.

**Lignes que le jeton ne peut pas voir.** GitHub répond `FORBIDDEN` pour un champ que le jeton ne peut pas voir sur une ligne particulière, comme les collaborateurs d'un dépôt sans accès en écriture, et `NOT_ORG_OWNED_REPO` pour un champ qui n'existe que sur les dépôts appartenant à une organisation. Ce champ est nul dans cette ligne, le reste de la lecture se poursuit et Provisa consigne un avertissement. Une erreur portant sur la table elle-même fait échouer la lecture.

**Pages lourdes.** Lorsque GitHub répond `RESOURCE_LIMITS_EXCEEDED` parce que le calcul d'une page coûte trop cher, la page est redemandée à la moitié de sa taille.

**Objets imbriqués.** Les tables GitHub utilisent leur propre profondeur d'imbrication, 0 (`max_object_depth=0` pour ce type de source), au lieu de `graphql_remote.max_object_depth`. Une colonne d'objet imbriqué est sélectionnée avec ses seuls champs scalaires, et les objets qu'elle contient apparaissent comme `__typename`.

**Stockage du jeton.** Le jeton est placé dans le coffre de secrets et la ligne de la source conserve une référence, de sorte qu'un redémarrage relit la source sans qu'il faille ressaisir le jeton.

**Fonctionnement (opérateurs).** Le schéma de GitHub est livré avec Provisa ; ajouter la source ne déclenche donc aucun appel d'introspection, et un grand schéma ne coûte rien à l'enregistrement. Les tables en sont mappées une à une au fur et à mesure de leur enregistrement. Le endpoint refresh refuse ce type de source ; un nouveau schéma GitHub arrive avec une version de Provisa.

---

### GitLab

GitLab est un type de source ordinaire, ajouté et enregistré comme GitHub : ajouter une source de type **GitLab** avec un jeton d'accès, puis enregistrer les tables souhaitées du schéma `graphql`. Le préfixe par défaut des noms de table est `gl`. La source atteint `gitlab.com`.

**Choisir les colonnes.** GitLab attribue un coût à chaque requête et refuse celle qui coûte trop cher : 200 points pour un appelant anonyme, 250 avec un jeton. Une table large avec toutes les colonnes sélectionnées dépasse ce coût ; enregistrez donc une table GitLab avec les colonnes que vous voulez.

- Lorsque vous enregistrez une table, Provisa demande une fois à GitLab s'il traitera la sélection à la taille de page qu'utilisent les lectures. Si GitLab répond que la requête est trop complexe ou trop volumineuse, la table n'est pas enregistrée et le résultat porte le message de GitLab : `Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.`
- Le coût d'une colonne dépend de sa nature. Une valeur simple coûte environ un point ; une colonne d'objet imbriqué coûte plusieurs fois plus. Retirer les colonnes d'objet imbriqué libère le plus.
- La taille de page fait partie du coût. Il s'agit de `graphql_remote.max_list_items`.

**Vérification du jeton.** GitLab répond à un jeton non reconnu par un résultat vide au lieu d'une erreur. Provisa le traite comme un jeton refusé et n'ajoute pas la source.

---

### Schéma distant gRPC {: #grpc-remote-schema }

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

Provisa récupère le proto, l'analyse avec un analyseur texte pur (sans dépendance proto externe au moment de l'analyse), compile les stubs Python via `grpc_tools.protoc`, et ouvre un `grpc.aio.Channel` persistant.

Les fichiers proto peuvent également être des chemins locaux. Les chemins d'importation pour les types bien connus (`google/protobuf/timestamp.proto`) sont stockés au moment de l'enregistrement et réutilisés lors de l'actualisation.

**Ce que la source propose.** Chaque méthode `rpc` du proto est classée comme query ou mutation selon trois signaux, par ordre de priorité :

1. **`method_overrides`** dans le payload d'enregistrement — `{"MethodName": "query"}` ou `{"MethodName": "mutation"}` est prioritaire sur tout le reste.
2. **`server_streaming: true`** — le serveur envoie un flux de messages ; c'est toujours une table virtuelle (sauf si la sortie est un scalaire).
3. **Le message de sortie possède un champ répété de type message** — p. ex. `ListOrdersResponse { repeated Order items; }` est traité comme un enveloppant de liste et devient une table virtuelle. Les champs scalaires répétés (p. ex. `repeated string tags`) ne déclenchent pas cette règle, car ce sont des propriétés de type tableau d'une entité unique, et non des sources de lignes.

Les méthodes qui ne correspondent à aucun de ces signaux (RPC unaire renvoyant un message d'entité unique, ou toute sortie scalaire) deviennent des fonctions suivies.

**Enregistrer des tables.** Chaque méthode de query est proposée comme une table, nommée `{namespace}__{Service}__{Method}`, sous le schéma de sélecteur `grpc_remote`. Enregistrez celles que vous voulez avec le sélecteur Register Table (`availableTables`, `availableColumns`, `registerTable`, comme pour les sources GraphQL), en choisissant les colonnes de réponse. Les champs de la requête deviennent des colonnes de filtre natif `_nf_*`, et celles-ci sont toujours incluses.

Les méthodes de mutation sont des commandes proposées, comptées dans `available_mutations` ; ajouter la source n'en enregistre aucune. Une mutation gRPC s'enregistre sur la page Commandes en choisissant la source puis la méthode, nommée `Service.Method` ; le genre de la commande est `source_operation`. Voir [Opération d'écriture d'une source distante](commands.md#a-remote-sources-write-operation).

**Nommage des tables.** Le nom par défaut est `{namespace}__{ServiceName}__{MethodName}`. En l'absence de namespace, les noms du service et de la méthode sont assemblés directement. Toute table enregistrée peut recevoir un `alias` ; lorsqu'il est défini, l'alias devient le nom utilisé partout (requêtes, SDL, relations). Le nom généré automatiquement constitue la clé d'enregistrement et ne change jamais.

**Correspondance des types.** Les types scalaires proto sont mis en correspondance avec les types SQL comme suit.

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

**Relations à l'enregistrement.** `relationships` fonctionne comme pour l'adaptateur GQL : il déclare des chemins de jointure FK/PK, stockés comme relations déclarées manuellement (sans indicateur `remote_managed`). Un rafraîchissement les laisse telles quelles.

**Méthodes de requête.** Les champs du message de sortie deviennent des colonnes de table. Les champs du message d'entrée deviennent à la fois des arguments GraphQL transmis à l'appel distant *et* des colonnes enregistrées avec le préfixe `_nf_` et `native_filter_type: "grpc_input"`, le même mécanisme utilisé par GQL et OpenAPI pour l'injection de filtres natifs.

**Sous-champs de messages imbriqués.** Pour les méthodes de requête, les champs de type message non répétés à la profondeur 0 (colonnes de sortie directes) voient leurs sous-champs résolus un niveau plus loin et stockés comme `object_fields` dans le `ColumnDef`. Ces métadonnées sont utilisées pour l'extraction de sous-champs `jsonb` en SQL et pour la documentation du schéma. Les champs imbriqués au-delà de la profondeur 1 ne sont pas développés récursivement.

Les méthodes en streaming côté serveur regroupent tous les messages diffusés en une liste avant de renvoyer les lignes.

**Méthodes de mutation.** Une méthode de mutation enregistrée est une commande dont les arguments sont les champs du message d'entrée, chacun typé `json` et transmis sans changement. La réponse du service distant revient sous forme de lignes ; un appel refusé est un 422, `functions.remote_refused`. Voir [Opération d'écriture d'une source distante](commands.md#a-remote-sources-write-operation).

**Gestion des canaux.** Un `grpc.aio.Channel` par source enregistrée est stocké dans l'état de l'application et réutilisé pour les requêtes suivantes. L'ancien canal est fermé avant qu'un nouveau ne s'ouvre lors de l'actualisation.

**Actualisation.** Envoyer une requête POST à `/admin/grpc-remote/refresh/{source_id}`. Recharge le proto depuis le chemin stocké, recompile les stubs et met à jour les tables déjà enregistrées par rapport au proto, avec les colonnes avec lesquelles chacune a été enregistrée. Elle n'enregistre aucune nouvelle table ; une méthode de query ajoutée au proto reste proposée. Il est également possible d'envoyer une requête PUT à `/admin/grpc-remote/{source_id}/proto` avec un nouveau `proto_text` pour mettre à jour le proto en ligne.

**Limitations.**

- L'extraction de sous-champs d'objet se limite à un niveau de profondeur. Les champs de message imbriqués au-delà de la profondeur 1 ne sont pas développés récursivement.

---

### OpenAPI / REST

**Comment ajouter la source.** Envoyer une requête POST à `/admin/openapi/register` avec un ID de source et une spécification, chargée depuis un fichier local ou une URL. La spécification est analysée et conservée avec la source ; aucune table ni command n'est enregistrée. La réponse indique `tables: 0` et `mutations: 0`, les compteurs proposés figurant dans `available_tables` et `available_mutations`.

**Enregistrer des tables.** Enregistrez chaque opération GET souhaitée via le sélecteur Register Table (`availableTables`, `availableColumns`, `registerTable`), en choisissant les colonnes. Enregistrez chaque opération non-GET individuellement comme commande sur la page Commandes, listées par `availableFunctions` ; voir [Opération d'écriture d'une source distante](commands.md#a-remote-sources-write-operation). `PUT /admin/openapi/spec/{source_id}` stocke une spécification modifiée à la main, n'enregistre rien et renvoie `available_tables` et `available_mutations`.

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

**Ce que propose la source.** Chaque opération GET de la spécification est proposée comme table, sauf si son schéma de réponse est un type scalaire (`string`, `number`, `boolean`, `integer`) ; un GET qui renvoie un scalaire est une fonction à une seule colonne `value`. Toute autre opération (POST, PUT, PATCH, DELETE) est proposée comme command, nommée par son `operationId`. Une fois enregistrée, elle prend les paramètres de chemin de l'opération et un argument `body` pour le corps de la requête, chacun de type `json`, et tout autre argument va dans la query string. Voir [Opération d'écriture d'une source distante](commands.md#a-remote-sources-write-operation).

Ordre de priorité de la classification : `operation_overrides` (payload) est prioritaire sur `x-provisa-kind` (extension de la spécification), elle-même prioritaire sur l'heuristique GET. `operation_overrides` est le chemin de surcharge recommandé ; `x-provisa-kind` est destiné aux cas où la spécification elle-même doit porter la classification.

**Relations à l'enregistrement.** `relationships` fonctionne comme pour les autres adaptateurs : stockées comme relations déclarées manuellement et conservées au rafraîchissement.

**Nommage des tables.** Les tables utilisent l'`operationId` de l'opération. En l'absence d'`operationId` défini, Provisa génère un slug `{method}_{path}`. Un alias est dérivé en supprimant le segment verbal initial et en mettant le nom au singulier (`findPetsByStatus` → `pet_by_status`).

**Correspondance des types.** Les types JSON Schema sont mis en correspondance avec les types Provisa comme suit.

| Type JSON Schema | Type Provisa |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**Paramètres en colonnes de filtre natif.** Les paramètres de chemin et de requête qui ne sont pas déjà des champs de la réponse deviennent des colonnes avec `native_filter_type` à `path_param` ou `query_param`, préfixées `_nf_`. Quand un paramètre porte le même nom qu'un champ de la réponse, ses métadonnées sont fusionnées dans l'entrée de cette colonne, sans créer de doublon.

**Résolution du schéma de réponse.** Le mapper vérifie `responses.200`, puis `responses.2xx`, puis `responses.default`. Les réponses de type tableau sont déballées vers leur schéma d'élément. Les références `$ref` sont résolues sur un niveau de profondeur.

**Sous-champs d'objet.** Les propriétés de réponse de `type: object` possédant leurs propres `properties` sont stockées comme `object_fields` sur la colonne. Ces sous-champs sont visibles dans le SDL et utilisés pour l'extraction `jsonb` dans les requêtes.

**Mise en cache des réponses.** Les résultats des opérations GET sont mis en cache dans PostgreSQL par `pg_cache.py`. Chaque combinaison de paramètres de requête obtient son propre groupe `_params_hash`. Les lignes d'un hash donné sont remplacées à l'expiration du TTL. Les endpoints à paramètre de chemin (`/pets/{id}`) omettent la récupération initiale en masse. La table de cache est créée vide pour l'introspection de schéma, puis peuplée par clé primaire au fur et à mesure des requêtes.

**Actualisation.** Envoyer une requête POST à `/admin/openapi/refresh/{source_id}`. Ré-analyse la spécification via `_load_and_register`, qui n'enregistre rien : elle n'ajoute ni table ni colonne. Les règles de gouvernance existantes sont préservées. Une table enregistrée conserve ses colonnes ; elle est lue via l'opération de la spec actualisée.

**Limitations.**

- L'extraction de sous-champs d'objet se limite à un niveau de profondeur. Les propriétés imbriquées dans `object_fields` ne sont pas développées récursivement.
- Les paramètres d'en-tête et de cookie sont ignorés ; seuls les paramètres `path` et `query` sont enregistrés.
- La résolution des `$ref` au niveau de la spécification se limite à un niveau de profondeur pour les schémas de propriétés ; les références de composants profondément imbriquées peuvent ne pas se résoudre.

---

## Impact de l'enregistrement d'une table distante {: #impact-of-registering-a-remote-table }

Une table enregistrée depuis n'importe quelle source de schéma distant est une table Provisa ordinaire. Rien ne la distingue, au moment de l'exécution, d'une table relationnelle connectée localement.

**Interfaces de requête.** La table est immédiatement interrogeable via GraphQL, SQL (pgwire ou direct), Cypher (GQL), JSON:API et Arrow Flight. La génération de schéma synthétise `ColumnMetadata` pour les tables distantes, puisqu'elles n'ont pas de catalogue ; la correspondance des types est appliquée au moment de la construction du schéma.

**Modèle de sécurité.** Les cinq couches de gouvernance s'appliquent :

1. Contrôle d'accès par domaine — le `domain_id` de la table détermine quels rôles peuvent la voir.
2. Sécurité au niveau des lignes (RLS) — les filtres de ligne configurés sur la table sont injectés dans chaque requête, quelle que soit l'interface.
3. Visibilité des colonnes — la liste `visible_to` de chaque colonne contrôle l'exposition des champs par rôle.
4. Masquage des colonnes — les règles de masquage s'appliquent à l'étape 2 du pipeline de gouvernance.
5. Protection des prédicats — les colonnes masquées sont rejetées des clauses WHERE et HAVING.

Les requêtes ad hoc sur des tables distantes sont autorisées sur les seuls droits de l'utilisateur. L'accès suit partout le même modèle fondé sur les droits (droits de table et de colonne plus relations approuvées), sans mode de gouvernance par table.

**Gouvernance des relations (V002).** Une condition JOIN sur des tables distantes, dans une requête SQL ou Cypher, doit correspondre à une relation enregistrée et approuvée. Les requêtes GraphQL sautent la vérification V002, car les relations définies dans le SDL sont approuvées par conception. Voir [docs/security.md](security.md#relationship-governance-v002).

**Colonnes de type OBJECT.** Lorsqu'une colonne correspond à un OBJECT GQL inline non gouverné ou à un type d'objet OpenAPI, son type Provisa est `jsonb`. La colonne stocke le blob JSON imbriqué intégral. Lorsque des sous-champs sont déclarés (`gql_object_fields` ou `object_fields`), la correspondance `gql_object_columns` est peuplée au moment de la construction du schéma. Le générateur SQL utilise cette correspondance pour émettre des expressions d'extraction `->>` pour les sous-champs lorsqu'une requête les sélectionne.

**Arguments obligatoires en tant que paramètres de filtre natif.** Les champs de requête racine dotés d'arguments non nuls et sans valeur par défaut injectent des colonnes supplémentaires sur la table enregistrée. Ces colonnes portent `native_filter_type: query_param`. Le traducteur Cypher réécrit `WHERE n.id = $val` en `WHERE n._nf_id = $val`, et l'exécuteur GraphQL les récupère comme variables à transmettre au endpoint distant.

---

## Impact de la création d'une relation de couverture {: #impact-of-creating-a-covering-relationship }

Lorsqu'un data steward enregistre une relation entre deux tables distantes (ou entre une table distante et une table locale), cette relation devient le chemin de jointure utilisé au moment de la requête.

**Comment la jointure l'emporte.** Lors de la compilation de la requête, Provisa résout le chemin de jointure via la relation enregistrée. `source_column` et `target_column` de la relation deviennent la condition de jointure dans le SQL généré. La jointure remplace tout appel distant par table qui serait autrement nécessaire pour le type connecté.

**Le blob brut n'est jamais exposé en SQL.** La colonne `breed` de `petstore__pets` ne peut pas être sélectionnée comme valeur jsonb brute dans une requête SQL. Dès qu'une relation est enregistrée entre `petstore__pets` et `petstore__breeds`, les requêtes SQL parcourent la jointure : `SELECT breed.name FROM petstore__pets` se résout par la jointure FK. Sans relation enregistrée mais avec des sous-champs déclarés sur la colonne (`gql_object_fields`), les références SQL aux sous-champs sont réécrites en extraction `->>` sur le blob stocké. Ce chemin n'existe que pour les types en ligne non gouvernés ; les champs dont la cible est gouvernée sont absents du SDL et n'ont pas de blob dont extraire. Le blob brut lui-même n'est jamais renvoyé comme simple valeur de colonne.

Dans le SDL GraphQL, un champ OBJECT en ligne non gouverné a pour type le type d'objet imbriqué. Qu'une jointure ou une extraction de blob le serve à l'exécution ne change pas la forme du SDL. Dès que le type enfant est enregistré comme table à part entière, et donc gouverné, les cinq couches de gouvernance s'y appliquent indépendamment : ses propres règles RLS, visibilité des colonnes, règles de masquage, gardes de prédicats et contrôle d'accès par domaine. L'extraction de blob contourne tout cela, car les données enfant arrivent intégrées à la ligne parente et seules les règles de la table parente les gouvernent. Pour gouverner finement le type enfant, enregistrez-le comme table et créez une relation.

**`graphql_alias` sur la relation.** Le champ `graphql_alias` nomme le champ du SDL que la relation expose sur le type parent. En son absence, le nom est dérivé du `field_name` de la table cible et de la cardinalité de la relation via `rel_field_name(target.field_name, cardinality)`.

**V002 sur le chemin de jointure.** Les requêtes SQL et Cypher qui parcourent la relation sont soumises à la gouvernance des relations V002. La relation doit être enregistrée et approuvée pour que la jointure soit autorisée. Le parcours GraphQL via le champ de relation du SDL est toujours préapprouvé.

**Indicateur remote-managed.** Les relations détectées automatiquement lors de l'enregistrement d'un schéma distant GraphQL sont stockées avec `remote_managed: True`. Il s'agit d'un marqueur de métadonnées ; il ne modifie pas le comportement de gouvernance.

---

## Comportement de définition de type seule {: #type-def-only-behavior }

Tous les types d'un schéma distant n'ont pas besoin d'être une table interrogeable.

Lorsque `root_table_ids` est défini sur un `SchemaInput`, les tables dont l'identifiant est absent de cet ensemble sont exclues des champs de requête racine dans le SDL généré. Elles restent présentes en tant que types GraphQL et sont accessibles via des champs de relation sur les tables disposant d'entrées racine.

Le même mécanisme s'applique quand le schéma est construit filtré par domaine. Les tables des domaines auxquels le rôle n'a pas accès ne sont que des définitions de type : leur définition de type existe dans le SDL pour parcourir les relations, mais aucun champ de requête racine n'est généré pour elles.

Une table en définition de type seule :

- N'a pas de champ de requête racine, donc les clients ne peuvent pas l'interroger directement par nom.
- Est accessible via des champs de relation sur les tables disposant d'entrées racine.
- Apparaît toujours dans l'introspection de schéma comme un type nommé.
- Conserve l'application de toutes les règles de gouvernance lorsque les données sont consultées via une relation.

Une table, définition de type comprise, ne quitte complètement le schéma que lorsque son enregistrement est supprimé. Réduire une table à sa définition de type (en retirant son ID de `root_table_ids`, ou par le filtrage d'accès aux domaines) ne retire pas le type.

Les intendants peuvent ainsi exposer des graphes d'objets navigables où certains types ne sont accessibles que par parcours et ne peuvent pas être interrogés seuls.
