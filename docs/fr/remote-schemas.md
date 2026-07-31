# Schémas distants

Une source de schéma distant connecte une API externe — GraphQL, gRPC ou REST (OpenAPI) — à la couche sémantique de Provisa. Une fois enregistrées, les opérations de l'API externe deviennent des tables et des fonctions Provisa de première classe. (REQ-308, REQ-316, REQ-325) Chaque règle de gouvernance, chaque interface de requête et chaque couche de sécurité s'applique automatiquement. (REQ-310, REQ-319, REQ-328) Le service distant ne voit jamais les règles de gouvernance de Provisa. (REQ-310, REQ-319, REQ-328)

---

## Trois types de source

### Schéma distant GraphQL (REQ-307–313)

**Comment l'enregistrer.** Envoyer une requête POST à `/admin/sources/graphql-remote` avec l'URL du endpoint, un namespace et une authentification facultative. Provisa déclenche une requête d'introspection `__schema` standard sur le endpoint distant. (REQ-307) [tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

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

**Ce qui est découvert automatiquement.** Chaque champ du type `Query` distant qui renvoie un OBJECT devient une table virtuelle. Chaque champ du type `Mutation` distant devient une fonction suivie. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**Nommage des tables.** Les tables sont nommées `{namespace}__{field_name}`. Avec le namespace `petstore` et un champ de requête `pets` : le nom de la table est `petstore__pets`. (REQ-312) [tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Correspondance des types (REQ-308).** Les champs scalaires sont directement mis en correspondance avec les types Provisa. Les champs OBJECT se répartissent en deux cas selon que le type cible est gouverné ou non (voir « Tables gouvernées » ci-dessous). [tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| Type GraphQL | Type Provisa |
|---|---|
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

**Arguments obligatoires.** Lorsqu'un champ de requête racine possède des arguments non nuls sans valeur par défaut, ceux-ci deviennent des colonnes `native_filter_type: query_param` sur la table (préfixées `_nf_` au moment de l'injection). L'exécuteur les transmet en tant que variables GraphQL. (REQ-555) [tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**Relations détectées automatiquement.** Provisa analyse les colonnes de type OBJECT de chaque table. Lorsque le type GQL référencé est également enregistré comme table dans la même source, une relation est émise. Les relations plusieurs-à-un déduisent les colonnes source et cible à partir de conventions de nommage (`breedName` sur le type source → `name` sur le type cible `Breed`). Les champs un-à-plusieurs (LIST) émettent des relations avec des références de colonne vides — la clé étrangère se trouve du côté cible. (REQ-554) [tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutations.** Les champs de mutation produisent des fonctions suivies avec des types d'argument dérivés des arguments de la mutation et un `return_schema` dérivé du type de retour de la mutation. (REQ-308) [tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**Actualisation.** Envoyer une requête POST à `/admin/sources/graphql-remote/{id}/refresh`. Ré-introspecte le schéma distant et met à jour les enregistrements de tables et de fonctions. Les règles de gouvernance existantes (RLS, masquage) sont préservées. (REQ-311) [tool-verified: `provisa/api/admin/graphql_remote_router.py:217–257`]

**Limitations.**
- Les champs de requête racine de type scalaire et ENUM (dont le type de retour n'est pas OBJECT) deviennent des fonctions suivies, et non des tables virtuelles. Leur `return_schema` est une colonne unique `value` du type scalaire correspondant. [tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- L'imbrication d'objets est résolue au moment de l'enregistrement jusqu'à `graphql_remote.max_object_depth` (par défaut : 5). La sélection de récupération distante et les métadonnées de sous-champs sont toutes deux construites jusqu'à cette profondeur ; les champs au-delà de la limite ne sont pas récupérés et ne sont pas disponibles pour l'extraction SQL. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:38–52`]
- Les champs OBJECT imbriqués de type LIST (p. ex. `breed.awards: [Award]`) sont inclus dans la sélection de récupération jusqu'à `graphql_remote.max_list_depth` niveaux d'imbrication (par défaut : 2). Dans cette limite, la liste est récupérée sous forme de tableau `jsonb` sur la colonne parente, et la sélection GQL injecte `first: N`, où N correspond à `graphql_remote.max_list_items` (par défaut : 100), afin de plafonner la taille du tableau. Au-delà de `max_list_depth`, le champ LIST est entièrement exclu afin d'éviter une expansion illimitée des données. En SQL, le tableau est accessible via `json_array_elements(column_name)` ou une extraction par index `->>`. Si le type d'élément de la liste possède sa propre requête racine, il est préférable de l'enregistrer comme table distincte et de créer une relation — le chemin de jointure est plus efficace et évite le blob. (REQ-556) [tool-verified: `provisa/graphql_remote/mapper.py:43–70`]
- Pour les requêtes SQL, les colonnes de type OBJECT non gouvernées sont récupérées intégralement depuis la source distante (tous les sous-champs jusqu'à la profondeur configurée) et mises en cache sous forme de `jsonb`. L'accès aux sous-champs en SQL est géré via une extraction `->>` sur le blob ; la requête distante n'est pas restreinte aux seuls champs sélectionnés par la requête SQL. Lorsque le type d'élément de la liste n'a pas de requête racine et que la représentation en blob est insuffisante, il convient d'écrire directement la requête en SDL GraphQL — Provisa reproduit fidèlement la sélection de champs GQL, de sorte que la source distante reçoit exactement les champs demandés. [tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- Si le serveur distant rejette un champ de type OBJECT parce qu'il exige une sélection de sous-champs (ce qui ne devrait pas se produire lorsque `gql_selection` est disponible), l'exécuteur retente une fois en retirant ces champs afin que les colonnes scalaires soient tout de même renvoyées. [tool-verified: `provisa/graphql_remote/executor.py:76–80`]

---

### Schéma distant gRPC (REQ-322–329)

**Comment l'enregistrer.** Envoyer une requête POST à `/admin/grpc-remote/register` avec l'adresse du serveur, un chemin ou une URL vers un fichier `.proto`, et une configuration TLS facultative.

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

**Ce qui est découvert automatiquement.** Chaque méthode `rpc` du proto est classée comme requête ou mutation à l'aide de trois signaux, par ordre de priorité : (REQ-323) [tool-verified: `provisa/grpc_remote/mapper.py`]

1. **`method_overrides`** dans le payload d'enregistrement — `{"MethodName": "query"}` ou `{"MethodName": "mutation"}` est prioritaire sur tout le reste.
2. **`server_streaming: true`** — le serveur envoie un flux de messages ; c'est toujours une table virtuelle (sauf si la sortie est un scalaire).
3. **Le message de sortie possède un champ répété de type message** — p. ex. `ListOrdersResponse { repeated Order items; }` est traité comme un enveloppant de liste et devient une table virtuelle. Les champs scalaires répétés (p. ex. `repeated string tags`) ne déclenchent pas cette règle — ce sont des propriétés de type tableau d'une entité unique, et non des sources de lignes.

Les méthodes qui ne correspondent à aucun de ces signaux (RPC unaire renvoyant un message d'entité unique, ou toute sortie scalaire) deviennent des fonctions suivies.

**Nommage des tables.** Le nom par défaut est `{namespace}__{ServiceName}__{MethodName}`. En l'absence de namespace, les noms du service et de la méthode sont assemblés directement. Toute table enregistrée peut recevoir un `alias` ; lorsqu'il est défini, l'alias devient le nom utilisé partout (requêtes, SDL, relations). Le nom généré automatiquement constitue la clé d'enregistrement et ne change jamais. (REQ-322) [tool-verified: `provisa/core/repositories/table.py:129–134`]

**Correspondance des types (REQ-324).** Les types scalaires proto sont mis en correspondance avec les types SQL comme suit. [tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Type Proto | Type SQL |
|---|---|
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

**Méthodes de mutation (REQ-326).** Les champs du message d'entrée deviennent des arguments d'entrée de la mutation. Le schéma du message de sortie devient le `return_schema`. [tool-verified: `provisa/grpc_remote/executor.py:122–143`]

**Gestion des canaux.** Un `grpc.aio.Channel` par source enregistrée est stocké dans l'état de l'application et réutilisé pour les requêtes suivantes. L'ancien canal est fermé avant qu'un nouveau ne s'ouvre lors de l'actualisation. (REQ-327) [tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**Actualisation.** Envoyer une requête POST à `/admin/grpc-remote/refresh/{source_id}`. Recharge le proto depuis le chemin stocké, recompile les stubs, et réenregistre les tables et les fonctions. Alternativement, envoyer une requête PUT à `/admin/grpc-remote/{source_id}/proto` avec un nouveau `proto_text` pour mettre à jour le proto en ligne. (REQ-329) [tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`, `provisa/api/admin/grpc_remote_router.py:300–358`]

**Limitations.**
- L'extraction de sous-champs d'objet se limite à un niveau de profondeur. Les champs de message imbriqués au-delà de la profondeur 1 ne sont pas développés récursivement. (REQ-556) [tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST (REQ-314–321)

**Comment l'enregistrer.** Appeler `auto_register_openapi_source` avec un identifiant de source, une spécification analysée et des métadonnées de connexion. La spécification est chargée depuis un fichier local ou une URL. (REQ-314) [tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/openapi/register.py:249–264`]

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

**Ce qui est découvert automatiquement.** Chaque opération GET de la spécification devient une table virtuelle, sauf si son schéma de réponse est un type scalaire (`string`, `number`, `boolean`, `integer`) — les opérations GET renvoyant un scalaire deviennent des fonctions suivies avec une colonne unique `value`. Chaque opération autre que GET (POST, PUT, PATCH, DELETE) devient une fonction suivie. (REQ-316, REQ-317)

Ordre de priorité de la classification : `operation_overrides` (payload) est prioritaire sur `x-provisa-kind` (extension de la spécification), elle-même prioritaire sur l'heuristique GET. `operation_overrides` est le chemin de surcharge recommandé ; `x-provisa-kind` est destiné aux cas où la spécification elle-même doit porter la classification. (REQ-408) [tool-verified: `provisa/openapi/mapper.py:192–203`]

**Relations au moment de l'enregistrement.** `relationships` fonctionne de manière identique aux autres adaptateurs — stockée comme des relations déclarées manuellement, préservées lors de l'actualisation. (REQ-554) [tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**Nommage des tables.** Les tables utilisent l'`operationId` de l'opération. En l'absence d'`operationId` défini, Provisa génère un slug `{method}_{path}`. Un alias est dérivé en supprimant le segment verbal initial et en mettant le nom au singulier (`findPetsByStatus` → `pet_by_status`). (REQ-557) [tool-verified: `provisa/openapi/register.py:39–56`]

**Correspondance des types.** Les types JSON Schema sont mis en correspondance avec les types Provisa comme suit. [tool-verified: `provisa/openapi/register.py:59–70`]

| Type JSON Schema | Type Provisa |
|---|---|
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

**Actualisation (REQ-321).** Réanalyser la spécification et rappeler `auto_register_openapi_source`. Les règles de gouvernance existantes sont préservées ; les enregistrements sont mis à jour via un upsert ON CONFLICT. [tool-verified: `provisa/openapi/register.py:249–264`]

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
