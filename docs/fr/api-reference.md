# Référence API

## Vue d'ensemble

Provisa expose des points de terminaison REST sous deux préfixes : `/data` pour l'exécution de requêtes et l'introspection de schéma, et `/admin` pour la gestion de la configuration. (REQ-043) La plupart des points de terminaison de données requièrent un identifiant de rôle. Les opérations de configuration admin utilisent une API GraphQL Strawberry à `/admin/graphql`. (REQ-164)

---

## Authentification

Lorsque `auth.provider` est configuré dans `provisa.yaml`, tous les points de terminaison à l'exception de `/health` et `/setup/status` requièrent un en-tête `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Sans authentification configurée, le serveur fonctionne en mode développement. Toute requête est traitée comme l'identité `anonymous`, qui correspond à tous les rôles configurés avec un accès domaine générique. (REQ-535)

**Connexion (`POST /auth/login`)** est fournie par le fournisseur d'authentification actif lorsque `provider: basic` est configuré. (REQ-124) Le format des identifiants et la réponse dépendent du fournisseur.

**Introspection d'identité :**

```http
GET /auth/me
```

Renvoie l'id, l'e-mail, le nom d'affichage, les appartenances aux organisations et les affectations de rôle de l'utilisateur authentifié. En mode développement, renvoie `dev_mode: true` avec la liste de tous les id de rôle. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Renvoie `{"provider": "<name>"}` ou `{"provider": null}` lorsque l'authentification n'est pas configurée. [tool-verified: `provisa/api/auth_router.py`]

---

## Points de terminaison de données

### `POST /data/graphql`

Exécute une requête ou une mutation GraphQL. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Corps de la requête :**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

Le champ `role` n'est utilisé qu'en mode développement (sans authentification). Lorsque l'authentification est active, le rôle de l'utilisateur authentifié est utilisé et `role` dans le corps est ignoré.

Le champ `extensions` prend en charge le protocole Automatic Persisted Query (APQ) : (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**En-têtes :**

- `X-Provisa-Role` — remplace le rôle (mode développement)
- `Accept` — format de réponse (voir Négociation de contenu)
- `Authorization` — `Bearer <token>` lorsque l'authentification est activée
- `X-Provisa-Redirect-Format` — type MIME pour la sortie de redirection S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — nombre de lignes au-delà duquel la redirection se déclenche (REQ-137)
- `X-Provisa-Redirect` — `true` pour forcer la redirection sans condition (REQ-029)

**Réponse (JSON en ligne) :**

```json
{
  "data": {
    "orders": [
      {"id": 1, "amount": 99.99}
    ]
  }
}
```

**Réponse (redirection) :**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

**Réponse (racines multiples avec mélange en ligne/redirection) :**

```json
{
  "data": {
    "orders": [{"id": 1}],
    "customers": null
  },
  "redirects": {
    "customers": {
      "redirect_url": "https://...",
      "row_count": 10000,
      "expires_in": 3600,
      "content_type": "application/vnd.apache.parquet"
    }
  }
}
```

Les requêtes à racines multiples exécutent chaque champ racine indépendamment. Les champs en dessous du seuil de redirection sont retournés en ligne ; ceux au-dessus sont redirigés. La clé `redirects` (au pluriel) associe les noms de champs aux informations de redirection. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**En-têtes de cache :**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (sur HIT) (REQ-536)

**Capacités requises :** `QUERY_DEVELOPMENT` pour toutes les requêtes, y compris l'introspection. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Négociation de contenu

| En-tête Accept | Format |
| --- | --- |
| `application/json` | JSON (par défaut) |
| `application/x-ndjson` | JSON délimité par des retours à la ligne |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirection

Les résultats dépassant un seuil de lignes configuré (ou lorsque `X-Provisa-Redirect: true`) sont écrits sur S3 et une URL pré-signée est renvoyée. (REQ-029, REQ-044)

| Format de redirection | Écrit par | Mémoire |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS fédéré | Aucune — les données ne transitent jamais par Provisa |
| `application/x-orc` | CTAS fédéré | Aucune — les données ne transitent jamais par Provisa |
| `application/json` | Provisa | Limité par la mémoire |
| `application/x-ndjson` | Provisa | Limité par la mémoire |
| `text/csv` | Provisa | Limité par la mémoire |
| `application/vnd.apache.arrow.stream` | Provisa | Limité par la mémoire |

Pour les exports analytiques volumineux, utilisez la redirection Parquet ou ORC. Le moteur de fédération écrit directement sur S3 en parallèle — aucune donnée ne transite par Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Exécute du SQL brut à travers le pipeline de gouvernance de l'étape 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Corps de la requête :**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Capacités requises :** `QUERY_DEVELOPMENT`.

Les violations de gouvernance sur `POST /data/sql` renvoient un statut HTTP 403. (REQ-002, REQ-266)

**Réponse :** même format que `/data/graphql` (lignes JSON par défaut, négociées via `Accept`).

---

### `POST /data/query`

Point de terminaison de requête unifié. Accepte GraphQL, SQL ou Cypher — la syntaxe est détectée automatiquement. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Les requêtes Cypher peuvent aussi être soumises au point de terminaison dédié `POST /query/cypher`. (REQ-345)

**Corps de la requête :**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Renvoie `{"data": ...}` pour GraphQL, `{"columns": [...], "rows": [...]}` pour SQL et Cypher.

---

### `POST /data/sql/explain`

Explique ou analyse une instruction SQL à travers le pipeline gouverné. (REQ-1519) [tool-verified: `provisa/api/data/endpoint_dev.py:328`]

Le point de terminaison enveloppe le SQL **gouverné** — l'instruction qui s'exécute réellement sous le rôle de l'appelant, après la sécurité au niveau des lignes et le masquage — dans la syntaxe EXPLAIN du dialecte. Ce que montre le plan est la version autorisée de la requête, pas l'entrée brute.

**Corps de la requête :**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Définissez `analyze: true` pour exécuter EXPLAIN ANALYZE. La requête s'exécute et le plan porte des nombres de lignes et des durées réels. Tous les dialectes ne prennent pas en charge ANALYZE ; voir le tableau dans [Plans de requête et statistiques](engines.md#query-plans-and-statistics).

**Réponse :** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` lorsque le dialecte ne prend pas en charge EXPLAIN, ou lorsque `analyze: true` est demandé sur un dialecte qui ne le prend pas en charge (p. ex. SQLite). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Renvoie l'état actuel du shard moteur sans le réveiller. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

L'interface interroge ce point de terminaison pour afficher une bannière de démarrage pendant que le moteur redémarre à froid. Cela ne déclenche jamais de réveil — l'interrogation est sûre et ne compte pas comme activité pour le récupérateur d'inactivité.

**Réponse :**

```json
{"state": "ready"}
```

Valeurs possibles :

| État | Signification |
| --- | --- |
| `always-on` | Bureau, auto-hébergé, ou coordinateur apporté par le client — aucune gestion de cycle de vie |
| `ready` | Le shard est actif et accepte les requêtes |
| `starting` | Démarrage à froid en cours |
| `stopped` | Le shard est réduit à zéro |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Déclenche un réveil du moteur sans exécuter de requête. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Renvoie `202 Accepted` immédiatement. Le réveil s'exécute en arrière-plan. Utilisez ceci si vous voulez que le moteur soit prêt avant l'arrivée de la première requête — par exemple, depuis un planificateur qui exécute des requêtes quelques minutes plus tard.

**Réponse :** `202 Accepted`, corps `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Point de terminaison REST simple auto-généré pour chaque table enregistrée. La chaîne de requête est mappée aux arguments GraphQL et la requête est compilée et exécutée via le même pipeline (sécurité au niveau des lignes, masquage, routage) que GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Paramètres de requête :**

- `limit` — nombre maximal de lignes (≥ 1)
- `offset` — nombre de lignes à ignorer (≥ 0)
- `fields` — noms de colonnes séparés par des virgules (par défaut, tous les champs scalaires)
- `filter` — tableau JSON d'objets de filtre `{"field", "comparator", "value"}`
- `orderBy` — tableau JSON d'objets de tri `{"field", "direction"}`

Le rôle authentifié est requis ; les requêtes non authentifiées renvoient `401`. Une spécification OpenAPI pour ces routes est servie à `GET /data/rest/openapi.json` avec l'interface Swagger UI à `GET /data/rest/docs`.

#### Explorateur OpenAPI / Swagger UI

La page de l'explorateur OpenAPI (`/app/openapi`) intègre Swagger UI dans un iframe en bac à sable. La spécification est délimitée par rôle — seules les tables et colonnes visibles pour le rôle actuel apparaissent — et optionnellement filtrée par domaine via le sélecteur de domaine. L'interface bascule automatiquement entre les thèmes clair et sombre. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

La page charge le HTML de la spécification via `fetch()` plutôt qu'un `src` d'iframe direct, de sorte que la requête porte le jeton porteur de la session et que les requêtes relatives propres à Swagger UI se résolvent correctement contre la même origine. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

Lorsqu'elle est atteinte depuis un lien NL « Ouvrir dans OpenAPI », la page développe automatiquement le point de terminaison ciblé, renseigne les paramètres de requête à partir de l'URL générée par NL (p. ex. `aggregate`, `groupBy`), et clique sur Exécuter — en utilisant un sondage du DOM pour s'assurer que chaque étape se termine avant que la suivante ne se déclenche. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Point de terminaison conforme à [JSON:API](https://jsonapi.org) auto-généré pour chaque table enregistrée. Même sécurité au niveau des lignes, masquage et routage que GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**En-tête `Accept` :** doit inclure `application/vnd.api+json` (le type MIME JSON:API) sinon la requête renvoie `406`.

**Paramètres de requête :**

- `fields[<type>]` — jeux de champs partiels, p. ex. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — p. ex. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — séparé par des virgules, préfixe `-` pour l'ordre décroissant, p. ex. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — pagination
- `aggregate` — fonctions d'agrégation séparées par des virgules à exécuter à la place de la récupération de lignes : `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Utilisez `?aggregate=count,sum` pour demander un sous-ensemble. Les réponses d'agrégation renvoient `data: null` avec les résultats dans `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — noms de colonnes séparés par des virgules ; utilisé avec `?aggregate=` pour regrouper les résultats. Seules les colonnes dans l'énumération `DistinctOnColumn` de la table sont valides ; le serveur renvoie `400` pour toute colonne que le rôle ne peut pas voir. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` pour inclure les colonnes scalaires de la table de base (et les scalaires de dimension jointe nommés dans `include=`) à l'intérieur du tableau `nodes` de chaque ligne de groupe. Requis lorsqu'une requête de regroupement NL demande aussi des détails de dimension. (REQ-1405)

Les réponses sont des objets ressources avec `type`/`id`/`attributes`. Les erreurs suivent la forme d'objet d'erreur JSON:API.

#### Explorateur JSON:API

La page de l'explorateur JSON:API (`/app/jsonapi`) est une interface navigateur sur ces points de terminaison. Sélectionnez une table dans la liste groupée par domaine, puis configurez :

- **Champs** — choisissez les colonnes à inclure (jeu de champs partiel) ; ne rien cocher pour demander chaque colonne
- **Relations** — sélectionnez les noms de relation dérivés de clé étrangère à charger via `?include=`
- **Filtre** — champ, opérateur (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) et valeur
- **Tri** — un champ, ascendant ou descendant
- **Agrégation** — choisissez les colonnes de regroupement dans la liste validée par le serveur, puis cochez une ou plusieurs fonctions d'agrégation ; lorsque des colonnes de regroupement sont sélectionnées, une case « Inclure les nœuds » ajoute les colonnes scalaires de la table de base à chaque ligne
- **Taille de page** — ressources par page, avec navigation première/précédente/suivante/dernière

Les résultats s'affichent dans une vue résumée mise en forme (fiches ressources avec ancres de relation cliquables) ou un onglet JSON brut. L'URL de la requête en direct est affichée et peut être copiée. La sélection de table et la taille de page persistent d'une session à l'autre dans `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

Lorsqu'elle est atteinte depuis un lien NL « Ouvrir dans JSON:API », l'explorateur présélectionne la table et amorce le sélecteur d'agrégation à partir des paramètres de requête générés par NL, puis exécute automatiquement la requête. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Soumet une question en langage naturel. Le service démarre une tâche asynchrone et renvoie `202 Accepted` avec un `job_id` immédiatement. Requiert un fournisseur LLM configuré sous la section de configuration `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Corps de la requête :**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Renvoie `{"job_id": "<id>"}`. Dépasser la limite de débit NL par rôle renvoie `429` avec un en-tête `Retry-After`. (REQ-370)

**Récupérer le résultat :**

- `GET /query/nl/{job_id}` — interrogation. Renvoie le document de la tâche.
- `GET /query/nl/{job_id}/stream` — SSE. Un événement `branch` par cible de génération à mesure qu'elle se termine, puis un événement `done`. (REQ-357, REQ-358)

Trois boucles de génération (Cypher, GraphQL, SQL) s'exécutent en parallèle, chacune validée par le compilateur et affinée en cas d'erreur. (REQ-355) L'invite est délimitée au schéma visible du rôle. (REQ-356) Le document de résultat classe chaque branche par cible : (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

```json
{
  "job_id": "<id>",
  "state": "complete",
  "branches": {
    "cypher":  {"query": "MATCH ...", "result": [...], "error": null},
    "graphql": {"query": "{ ... }",   "result": {...}, "error": null},
    "sql":     {"query": "SELECT ...", "result": [...], "error": null}
  }
}
```

Une branche qui épuise sa limite d'itérations renvoie `query: null`, `result: null`, et une chaîne `error`. Chaque requête générée s'exécute sous les droits du consommateur avec la gouvernance de l'étape 2 appliquée — le service ne contourne jamais la gouvernance. (REQ-359)

#### Regroupement NL avec détails de dimension (REQ-1405)

Lorsqu'une requête de regroupement NL projette aussi des colonnes d'une table de dimension jointe — par exemple, « nombre de demandes par utilisateur avec nom et e-mail de l'utilisateur » — l'exécuteur dérive des chemins pointés par champ (`dim_paths`) à partir des colonnes de dimension projetées dans le SELECT. Ces chemins alimentent le paramètre `includeNodes=` sur les URL générées par les panneaux JSON:API et OpenAPI, de sorte que ces panneaux demandent les mêmes champs de dimension jointe que les branches SQL et GraphQL ont résolus. Sans cela, `includeNodes=true` ne renverrait que les champs scalaires propres de la table d'agrégation de base. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

Sur le panneau gRPC, le `{Type}GroupByRequest` généré porte `include_nodes` (booléen) et `include` (chaîne répétée de noms de champs de relation). Le `{Type}GroupByRow` renvoyé inclut un champ `nodes` typé avec les lignes de détail de dimension. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Renvoie le SDL GraphQL pour le schéma d'un rôle. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**En-têtes :** `X-Role: <role_id>` (requis)

**Paramètres de requête :**

- `domain` — id de domaine séparés par des virgules. Lorsque défini, la réponse est filtrée au(x) domaine(s) nommé(s) et aux tables accessibles depuis eux.

**Réponse :** SDL GraphQL en `text/plain`.

---

### `GET /data/introspection`

Renvoie le JSON d'introspection GraphQL, optionnellement filtré par domaine. [tool-verified: `provisa/api/data/sdl.py:200`]

**En-têtes :** `X-Provisa-Role: <role_id>` (requis)

**Paramètres de requête :** `domain` — id de domaine séparés par des virgules.

**Réponse :** résultat d'introspection en `application/json`.

---

### `GET /data/graph-schema`

Renvoie la vue en graphe du schéma du rôle : étiquettes de nœud et leurs types de relation, pour les clients Cypher/graphe. Inclut `pk_columns` par étiquette de nœud afin que les appelants puissent déterminer les colonnes de clé primaire. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Réponse :** `application/json` avec `node_labels` (chacune portant `pk`/`pk_columns`) et `relationship_types`.

Chaque type de relation porte aussi `junction_table_name` et `properties` (REQ-1586). Sur une arête reposant sur une table de jonction, le premier nomme la table associative qu'elle parcourt et le second liste les colonnes de cette table lisibles comme `r.attr` et filtrables dans `WHERE` ; sur une arête reposant sur une clé étrangère, le nom est `null` et la liste de propriétés est vide, ce qui permet à un client de distinguer les deux cas. La table de jonction elle-même n'est jamais une étiquette de nœud — elle est l'arête, elle n'a donc aucune pastille dans un client de graphe ni aucune ligne dans `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Renvoie les id de domaine accessibles au rôle demandeur. [tool-verified: `provisa/api/data/sdl.py:116`]

**En-têtes :** `X-Role: <role_id>` (requis)

**Réponse :** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Renvoie la chaîne de version de schéma actuelle. Combine un nonce par démarrage avec un compteur de reconstruction. Les clients utilisent ceci pour invalider les caches de schéma après les redémarrages du serveur. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Réponse :** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Renvoie le fichier `.proto` auto-généré pour un rôle. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Réponse :** schéma protobuf en `text/plain`.

Chaque table enregistrée produit un `message` proto. Les relations produisent des champs de message imbriqués. Mappage de types : `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Flux Server-Sent Events pour les notifications de changement en temps réel d'une table. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

La livraison des notifications utilise un fournisseur enfichable choisi selon le type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY` (via asyncpg), les sources MongoDB utilisent Change Streams (`collection.watch()`), et les sources Kafka utilisent des groupes de consommateurs. Chaque fournisseur implémente une interface de surveillance asynchrone commune. Le filtrage par sécurité au niveau des lignes et la validation de schéma s'appliquent quel que soit le fournisseur. (REQ-258) Les sources WebSocket et RSS sont également prises en charge. (REQ-338, REQ-342)

**En-tête — `X-Provisa-Sink` :** définissez-la sur une cible Kafka (p. ex. `kafka://broker:9092/topic`) pour rediriger les événements de changement vers un puits Kafka au lieu de la réponse SSE. Le serveur lance un consommateur de puits et renvoie `202 Accepted` plutôt qu'un flux ouvert. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Points de terminaison REST admin

### Config

#### `GET /admin/config`

Télécharge le `provisa.yaml` actuel en `application/x-yaml` avec un en-tête `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Téléverse un YAML de configuration révisé. Le serveur écrit une sauvegarde `.bak`, enregistre le nouveau fichier, et recharge tous les schémas, sources et vues matérialisées. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Corps de la requête :** contenu YAML brut.

**Réponse :**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

En cas d'échec du rechargement : `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Télécharge la **configuration en direct actuelle** — la configuration telle que Provisa l'écrirait aujourd'hui, reflétant chaque table, relation, domaine, rôle et règle RLS créés par l'admin et accumulés depuis le démarrage. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

Le fichier sur disque peut être en retard sur l'état en direct si des changements ont été effectués via l'API admin sans téléversement ultérieur. Ce point de terminaison comble cet écart : sa sortie est ce que `PUT /admin/config` devrait recevoir pour que le fichier sur disque corresponde à l'état en direct.

Renvoie `application/x-yaml` avec `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Renvoie les deux côtés du diff de configuration — `original` (la référence de démarrage) et `current` (l'état en direct) — normalisés de façon identique, afin que la comparaison ne montre que les changements réels, pas le réordonnancement ou la dérive des commentaires. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Réponse :**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Génère un correctif diff unifié de la référence vers la configuration postée. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Envoyez le YAML révisé comme corps de la requête. La réponse est un fichier `text/x-patch` (`provisa.config.patch`) que `git apply` ou `patch` peut consommer directement — utile pour valider les changements de configuration pilotés par l'interface via un pipeline CI/CD.

---

### Paramètres

#### `GET /admin/settings`

Renvoie les paramètres de plateforme actuels en JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

**Réponse :**

```json
{
  "redirect": {
    "enabled": true,
    "threshold": 10000,
    "default_format": "application/vnd.apache.parquet",
    "ttl": 3600
  },
  "sampling": {
    "default_sample_size": 1000
  },
  "cache": {
    "default_ttl": 300
  },
  "naming": {
    "domain_prefix": false,
    "convention": "apollo_graphql"
  },
  "relationships": {
    "auto_track_fk": true
  },
  "otel": {
    "endpoint": "http://otel-collector:4318",
    "service_name": "provisa",
    "sample_rate": 1.0,
    "support_endpoint": "",
    "support_redact_sql_literals": true,
    "support_redact_attributes": []
  }
}
```

#### `PUT /admin/settings`

Met à jour les paramètres de plateforme à l'exécution. Tous les champs sont optionnels — seules les clés présentes dans le corps sont mises à jour. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

**Corps de la requête (exemple partiel) :**

```json
{
  "otel": {
    "support_endpoint": "https://telemetry.vendor.com/v1/traces",
    "support_redact_sql_literals": true,
    "support_redact_attributes": ["db.statement", "user.email"]
  },
  "cache": {"default_ttl": 600}
}
```

Champs modifiables par section :

- `redirect` : `enabled`, `threshold`, `default_format`, `ttl`
- `sampling` : `default_sample_size`
- `cache` : `default_ttl`
- `naming` : `domain_prefix`, `convention` — écrit dans le fichier de configuration et déclenche un rechargement de schéma (REQ-253)
- `relationships` : `auto_track_fk` — gouverne uniquement le suivi des clés étrangères. Une relation reposant sur une table de jonction est déclarée à l'enregistrement de la table et n'est jamais inférée, donc ce paramètre ne l'atteint pas. (REQ-1586)
- `otel` : `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Réponse :**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Modèles IA

#### `GET /admin/ai-models`

Renvoie les affectations de modèles IA de l'organisation agissante, le registre des modèles vectoriels, et la limite de débit NL. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

**Réponse :**

```json
{
  "ai_models": {
    "nl": "claude-3-5-sonnet-20241022",
    "embedding": "text-embedding-3-small"
  },
  "vector_models": [...],
  "nl": {"rate_limit": 20},
  "api_keys_set": {"anthropic": true, "openai": false}
}
```

Les clés API ne sont jamais renvoyées — `api_keys_set` rapporte uniquement si chaque fournisseur a une clé configurée. Les changements prennent effet à la prochaine requête ; aucun redémarrage n'est nécessaire. (REQ-1349)

#### `PUT /admin/ai-models`

Met à jour les affectations de modèles IA de l'organisation, le registre des modèles vectoriels, ou la limite de débit NL. Prend effet à la prochaine requête. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Renvoie les noms de modèles actuellement servis par un fournisseur, pour le sélecteur de modèle. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

La liste est lue en direct depuis la propre API de liste de modèles du fournisseur en utilisant la clé configurée de l'organisation — ou l'identifiant de déploiement lorsqu'aucune clé d'organisation n'est définie. Un modèle publié après la sortie de cette version est sélectionnable le jour même où le fournisseur le sert.

Renvoie `400` lorsque le fournisseur ne publie aucune API de liste de modèles (saisissez le nom du modèle directement dans ce cas) ou lorsqu'aucune clé n'est disponible. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Moteur de fédération

#### `GET /admin/federation-engine`

Renvoie la sélection actuelle du moteur de fédération, sa configuration de connexion, et le registre complet des moteurs sélectionnables. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

**Réponse :**

```json
{
  "current": "trino",
  "persisted": "trino",
  "registry": [
    {"key": "trino", "label": "Trino (embedded)", "fields": [...]},
    {"key": "duckdb", "label": "DuckDB", "fields": []}
  ],
  "note": "Changing the federation engine takes effect after the service is restarted."
}
```

La clé `current` est le moteur en cours d'exécution en ce moment ; `persisted` est ce qui est écrit dans le fichier de configuration et sera chargé au prochain redémarrage. Elles divergent lorsque la configuration a été changée mais que le service n'a pas encore redémarré.

#### `PUT /admin/federation-engine`

Persiste une sélection de moteur de fédération. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Corps de la requête :**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

La sélection est écrite dans la configuration de plateforme. Elle prend effet après le prochain redémarrage du service — le moteur est choisi une seule fois au démarrage.

---

### Politique de domaine

#### `POST /admin/domain-policy`

Change la politique de domaine de l'organisation agissante (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

Il s'agit d'une opération destructive délimitée à l'organisation agissante. Chaque source, table, domaine et relation enregistrés sont purgés et reconstruits sous la nouvelle politique. Utilisez ceci pour faire passer une organisation d'un mode à espace de noms de domaine à un mode plat (ou inversement).

**Corps de la requête :**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` efface le remplacement de l'organisation et revient au paramètre au niveau du déploiement. `use_domains: false` requiert `default_domain` (le domaine unique dans lequel toutes les tables atterrissent). La reconstruction du catalogue est synchrone ; la réponse revient une fois les schémas prêts.

---

### Observabilité

#### `GET /admin/traces/recent`

Renvoie jusqu'à N portées récentes complétées depuis le tampon de portées en mémoire. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Paramètres de requête :** `limit` (par défaut 50, max 200)

**Réponse :** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Recharge à chaud un catalogue nommé dans le coordinateur du moteur de fédération via son API REST. Reconnecte la connexion interne de Provisa et réexécute le DDL OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Paramètres de requête :** `catalog` (par défaut `"otel"`)

**Réponse :**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Redémarre le conteneur du moteur de fédération (développement mono-nœud uniquement). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Paramètres de requête :** `container` (par défaut la variable d'environnement `QUERY_ENGINE_CONTAINER`, puis `"trino"`)

---

### Découverte

#### `POST /admin/discover/relationships`

Déclenche la découverte de relations. Exécute toujours l'introspection de clé étrangère depuis le moteur de fédération. (REQ-018) Exécute l'inférence LLM si `ANTHROPIC_API_KEY` est défini. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Corps de la requête :**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` doit être l'un de `"table"`, `"domain"`, `"cross-domain"`. Pour la portée `"table"`, `table_id` (entier) est requis. Pour la portée `"domain"`, `domain_id` est requis.

**Réponse :** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Liste les candidats de relation en attente. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Accepte un candidat et l'enregistre comme relation. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Corps de la requête (optionnel) :** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rejette un candidat. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Corps de la requête :** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Renvoie le nombre de candidats rejetés. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Supprime tous les candidats rejetés. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Exploration de source

#### `POST /admin/sources/crawl`

Explore une source de données pour introspecter son schéma et enregistrer des tables. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Recherche de tables de source

#### `GET /admin/sources/{source_id}/tables/search`

Recherche des tables disponibles (pas encore enregistrées) dans une source par nom. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Profilage de tables

#### `POST /admin/tables/{table_id}/profile`

Exécute un profil de colonne sur une table enregistrée — cardinalité, min/max, taux de valeurs nulles. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Descriptions de source

#### `POST /admin/source-meta/db-description`

Génère des descriptions assistées par LLM pour les tables et colonnes d'une source. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Stockage objet (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Rapporte l'empreinte de stockage de l'organisation agissante par rapport à son allocation de plateforme, et si l'organisation a enregistré son propre magasin. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Lorsque l'organisation a enregistré son propre DSN, ses matérialisations y vont et ne sont plus comptées dans l'allocation. Le DSN lui-même n'est jamais renvoyé.

#### `PUT /admin/org-storage`

Enregistre (ou efface) le magasin de matérialisation propre à l'organisation. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Corps de la requête :**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

Le DSN est validé contre le moteur de fédération avant d'être accepté — un DSN inutilisable échoue à l'enregistrement, pas des heures plus tard lors d'un rafraîchissement. La valeur est chiffrée au repos et jamais renvoyée par GET.

Envoyez `storage_url: null` pour effacer le magasin propre de l'organisation et renvoyer ses matérialisations au magasin de plateforme (et à l'allocation). L'exécution de l'organisation est reconstruite dans le même appel, de sorte que le nouveau magasin est effectif immédiatement. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Chiffrement d'organisation (REQ-1574)

#### `GET /admin/org-encryption`

Renvoie l'état de clé actuel de l'organisation : empreinte, id, et provenance. Ne renvoie jamais le matériel de clé. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Lorsque l'organisation n'a défini aucune clé, renvoie `{"configured": false}`. Chaque organisation démarre dans cet état et hérite de la clé du déploiement.

#### `PUT /admin/org-encryption`

Définit ou fait pivoter la clé de chiffrement au repos de l'organisation. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Corps de la requête :**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omettez `key_b64` pour que Provisa génère une clé — la voie la plus sûre, car la clé n'apparaît jamais dans un presse-papiers ou un journal de requêtes. Fournir `key_b64` permet d'apporter votre propre clé.

La rotation ajoute une nouvelle entrée active à l'anneau de clés et conserve l'ancienne, de sorte que les données écrites sous la clé précédente restent lisibles. La rotation n'est pas un rechiffrement. Il n'existe aucun point de terminaison de suppression : retirer la dernière clé rendrait chaque charge utile enveloppée illisible. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

L'anneau en direct est reconstitué dans le même appel, de sorte que la prochaine écriture chiffrée utilise immédiatement la nouvelle clé.

---

### Import Hasura / DDN (REQ-1483)

#### `POST /admin/import/hasura/preview`

Convertit une archive de projet Hasura v2 ou DDN en configuration Provisa proposée sans rien écrire. [tool-verified: `provisa/api/admin/import_router.py`]

**Corps de la requête :**

```json
{
  "filename": "my-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` est `"auto"` (détecté depuis la structure de l'archive), `"hasura_v2"`, ou `"ddn"`.

**Réponse :**

```json
{
  "config_yaml": "...",
  "warnings": ["..."],
  "summary": {
    "sources": 1, "domains": 2, "tables": 40,
    "columns": 180, "roles": 3, "relationships": 15, "rls_rules": 6
  }
}
```

Rien n'est persisté. L'aperçu n'est pas mis en cache côté serveur ; `apply` prend le YAML que vous fournissez, de sorte que ce qui s'applique est exactement ce qui a été revu (et optionnellement édité).

#### `POST /admin/import/hasura/apply`

Charge une configuration précédemment prévisualisée dans l'organisation agissante. [tool-verified: `provisa/api/admin/import_router.py`]

**Corps de la requête :**

```json
{"config_yaml": "<yaml string>"}
```

Utilise le même chemin de rechargement à chaud que `PUT /admin/config`. Le catalogue, les schémas et les pools de l'organisation sont reconstruits avant que la réponse ne soit renvoyée.

---

### Échange Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Exporte le modèle gouverné de l'organisation en tant que document YAML Apache Ossie (incubating). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Le document est dérivé de l'état en direct à chaque requête — jamais mis en cache — donc il ne peut pas être périmé. Les tables deviennent des objets `dataset`, les colonnes deviennent des objets `field`, et les relations sont mappées vers des objets `relationship` Ossie.

Renvoie `text/yaml` avec `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

Analyse un document Ossie YAML ou JSON et renvoie des propositions d'enregistrement de table et de relation. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

**Corps de la requête :** YAML ou JSON Ossie brut. Le format est détecté automatiquement.

**Réponse :**

```json
{
  "proposals": {
    "tables": [...],
    "relationships": [...]
  }
}
```

Rien n'est enregistré. Utilisez l'écran de revue de l'interface admin pour accepter ou retrancher des propositions avant toute mutation.

---

### Actions (fonctions et webhooks)

Tous les points de terminaison sont sous le préfixe `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Chaque invocation — depuis GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, et Provisa gRPC — passe par un seul exécuteur gouverné qui applique `writable_by` et la gouvernance de façon uniforme. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Voir [docs/integrations.md](integrations.md#invoquer-des-commandes-a-travers-les-protocoles) pour la syntaxe d'appel par protocole.

#### `GET /admin/actions`

Renvoie toutes les fonctions DB et webhooks suivis. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

**Réponse :**

```json
{
  "functions": [
    {
      "name": "random_python_set",
      "implKind": "python",
      "binding": {"callable": "demo.py_functions:random_dataset"},
      "returns": "",
      "returnSchema": {
        "type": "array",
        "items": {"type": "object", "properties": {"id": {"type": "integer"}, "region": {"type": "string"}}}
      },
      "arguments": [{"name": "rows", "type": "Int"}, {"name": "seed", "type": "Int"}],
      "visibleTo": ["admin"],
      "writableBy": [],
      "domainId": "pet-store",
      "description": "Demo Python command returning random rows",
      "kind": "query"
    }
  ],
  "webhooks": [
    {
      "name": "add-pet",
      "url": "https://petstore.example.com/pets",
      "method": "POST",
      "kind": "mutation",
      "approved": true
    }
  ]
}
```

Chaque objet webhook porte un booléen `approved`. Un webhook est approuvé une fois qu'un steward exécute sa demande de création (REQ-209) ; les webhooks déclarés en configuration sont auto-approuvés. Un webhook non approuvé est enregistré mais n'est exposé sur aucune surface. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Enregistre une fonction suivie (commande). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Champs clés :**

| Champ | Requis | Description |
| --- | --- | --- |
| `name` | Oui | Nom de commande unique |
| `kind` | Oui | `"query"` → champ GraphQL Query ; `"mutation"` → champ Mutation |
| `implKind` | Non | Comment la commande s'exécute — voir le tableau ci-dessous (par défaut `source_procedure`) |
| `binding` | Non | Détails de connexion spécifiques à `implKind` (objet JSON) |
| `returnSchema` | Non | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — rend la commande à ensemble de résultats sur chaque surface |
| `arguments` | Non | Définitions d'arguments `[{name, type}]` ; l'ordre positionnel compte pour les appelants SQL et Bolt |
| `visibleTo` | Non | Id de rôle pouvant appeler la commande |
| `writableBy` | Non | Id de rôle autorisés à l'invoquer comme mutation |
| `domainId` | Non | Domaine pour le placement GraphQL et le contrôle d'accès |

**Valeurs `implKind` :**

| `implKind` | Ce qui s'exécute | Champs `binding` |
| --- | --- | --- |
| `source_procedure` | Procédure stockée sur une source enregistrée (par défaut) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script côté serveur | `script` |
| `http` | Appel HTTP sortant | `url`, `method` |
| `grpc` | Appel gRPC sortant vers un serveur externe | `target`, `method` |
| `python` | Callable Python hébergé par Provisa (REQ-885) | `callable` (p. ex. `"demo.py_functions:random_dataset"`) |

Les commandes de démonstration `random_python_set` (`implKind: python`) et `random_grpc_set` (`implKind: grpc`) illustrent en pratique les commandes à ensemble de résultats avec `returnSchema` ; les deux figurent dans `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Met à jour une fonction suivie par nom. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Supprime une fonction suivie par nom. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Enregistre un webhook suivi. (REQ-209) Enregistrer ou mettre à jour un webhook met en file une demande d'approbation de steward — le webhook devient actif sur toutes les surfaces uniquement après qu'un steward l'approuve. Les webhooks déclarés en configuration sont auto-approuvés. **Champs du corps de la requête :** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Met à jour un webhook suivi par nom. Toute modification réinitialise l'approbation en attente jusqu'à réapprobation. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Supprime un webhook suivi par nom. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Teste une action (fonction ou webhook) par nom. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Rôles

Tous les points de terminaison sont sous le préfixe `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Liste tous les rôles |
| `POST` | `/admin/roles/` | Crée un rôle |
| `PUT` | `/admin/roles/{role_id}` | Met à jour un rôle |
| `DELETE` | `/admin/roles/{role_id}` | Supprime un rôle |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Utilisateurs

Tous les points de terminaison sont sous le préfixe `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `POST` | `/admin/users/` | Crée un utilisateur local |
| `GET` | `/admin/users/` | Liste les utilisateurs locaux |
| `GET` | `/admin/users/{user_id}` | Obtient un utilisateur |
| `PUT` | `/admin/users/{user_id}` | Met à jour un utilisateur |
| `PATCH` | `/admin/users/{user_id}/password` | Change le mot de passe |
| `DELETE` | `/admin/users/{user_id}` | Supprime un utilisateur |
| `GET` | `/admin/users/{user_id}/assignments` | Liste les affectations de rôle |
| `POST` | `/admin/users/{user_id}/assignments` | Ajoute une affectation de rôle |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Supprime une affectation de rôle |

---

### Organisations

Tous les points de terminaison sont sous `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Liste les organisations |
| `POST` | `/admin/orgs/` | Crée une organisation |
| `PUT` | `/admin/orgs/{org_id}` | Met à jour une organisation |
| `DELETE` | `/admin/orgs/{org_id}` | Supprime une organisation |
| `GET` | `/admin/orgs/{org_id}/members` | Liste les membres |
| `POST` | `/admin/orgs/{org_id}/members` | Ajoute un membre |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Supprime un membre |

---

### Invitations

Tous les points de terminaison sont sous `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Crée une invitation |
| `GET` | `/admin/invites/` | Liste les invitations en attente |
| `DELETE` | `/admin/invites/{token}` | Révoque une invitation |

---

### GraphQL admin

#### `POST /admin/graphql`

Point de terminaison GraphQL Strawberry pour toutes les opérations admin : CRUD de source et de table, gestion des relations, configuration de domaine, règles RLS, contrôle du cache, conventions de nommage, gestion des tâches planifiées, et compilation de requêtes. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**Mutations clés :**

```graphql
# Cache
mutation { update_source_cache(source_id: "sales-pg", enabled: true, ttl: 600) { success } }
mutation { update_table_cache(table_id: 1, ttl: 60) { success } }

# Naming conventions
mutation { update_source_naming(source_id: "legacy-db", convention: "camelCase") { success } }
mutation { update_table_naming(table_id: 1, convention: "PascalCase") { success } }

# Scheduled tasks
mutation { toggle_scheduled_task(name: "daily-report", enabled: false) { success } }

# Compile a query (returns enforcement metadata and routed SQL)
mutation {
  compile_query(input: {role: "admin", query: "{ orders { id } }"}) {
    sql semantic_sql trino_sql direct_sql route route_reason sources root_field
    enforcement { rls_filters_applied columns_excluded masking_applied }
  }
}
```

[tool-verified: `provisa/api/admin/schema.py`, `provisa/api/admin/actions_router.py`]

---

### Configuration initiale

#### `GET /setup/status`

Renvoie l'état de configuration au premier lancement. Toujours non authentifié. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Termine la configuration au premier lancement. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Vérification de santé

#### `GET /health` ou `HEAD /health`

Renvoie `{"status": "ok"}`. Toujours non authentifié. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Réponses d'erreur

| Statut | Signification |
| --- | --- |
| 400 | Requête invalide, erreur de validation, ou erreur d'analyse SQL |
| 401 | Jeton d'authentification manquant ou invalide |
| 403 | Capacités insuffisantes ; violation de gouvernance |
| 404 | Rôle, ressource, ou fichier de configuration introuvable |
| 422 | En-tête requis manquant (p. ex. `X-Role`) |
| 503 | Base de données ou source non connectée ; dépendance indisponible |
| 504 | Délai de requête dépassé |

Les violations de gouvernance sur `POST /data/sql` renvoient un statut HTTP 403 avec un corps structuré : (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

Toutes les autres erreurs utilisent : `{"detail": "<message>"}`.

---

## Point de terminaison Arrow Flight

Port `8815`. Transport columnaire Arrow natif sur gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Les requêtes et la découverte de catalogue sont toutes deux disponibles sur la même connexion. Le pipeline de gouvernance complet (sécurité au niveau des lignes, masquage, échantillonnage) est appliqué à chaque requête. (REQ-130, REQ-143)

**Format du ticket** (JSON) :

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Utilisation (Python) :**

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")
ticket = flight.Ticket(b'{"query": "{ orders { id amount } }", "role": "admin"}')
# Stream batch-by-batch
for batch in client.do_get(ticket):
    process(batch.data)
# Or read all at once
table = client.do_get(ticket).read_all()
```

Lorsque le proxy Zaychik Flight SQL est disponible (port 8480), les lots d'enregistrements circulent de bout en bout sans matérialisation complète. (REQ-144) Repli sur la matérialisation via la couche de requête fédérée si Zaychik n'est pas disponible. (REQ-146)

---

## Point de terminaison gRPC Protobuf

Port `50051` (remplacer avec la variable d'environnement `GRPC_PORT` ou la configuration `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Passez le rôle dans la clé de métadonnées gRPC `x-provisa-role`. Si absente, le serveur abandonne avec `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Téléchargez le proto spécifique au rôle depuis `GET /data/proto/{role_id}`. Seules les tables et colonnes visibles pour ce rôle apparaissent. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Chaque table produit un RPC de flux `Query{TypeName}`. Les RPC `Insert{TypeName}` existent pour la symétrie de schéma mais abandonnent avec `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` est activé pour la découverte de service sans proto précompilé. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Le serveur gRPC ne démarre que lorsqu'un proto valide peut être compilé au démarrage. Si la construction du schéma échoue, le serveur gRPC ne démarre pas. (REQ-529)

#### RPC d'agrégation et de regroupement (REQ-1359, REQ-1361, REQ-1405)

Lorsqu'une table a `enable_aggregates` défini, le proto généré inclut deux RPC supplémentaires aux côtés de `Query{TypeName}` :

- **`Query{TypeName}Aggregate`** — renvoie des scalaires d'agrégation pour la table (`count` ; `sum`, `avg`, `stddev`, `variance` par colonne numérique ; `min`, `max` par colonne comparable)
- **`Query{TypeName}GroupBy`** — renvoie une ligne par clé de groupe avec des sous-champs d'agrégation et, optionnellement, des scalaires de table de base et des lignes de dimension jointe dans un champ `nodes`

Les deux passent par le même pipeline d'agrégation du compilateur que les champs racines `{field}_aggregate` et `{field}_group_by` de GraphQL — aucune implémentation d'agrégation distincte. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Champ `funcs` (REQ-1361).** Le message de requête accepte un champ de chaîne répétée `funcs`. Les valeurs valides sont `count`, `sum`, `avg`, `stddev`, `variance`, `min`, et `max`. Lorsque `funcs` est omis, chaque fonction que le schéma expose pour cette table est demandée. Lorsqu'il est défini, seules les fonctions nommées apparaissent. Si aucune des fonctions nommées ne s'applique aux types de colonnes de la table, la requête se replie sur `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Champs `include_nodes` et `include` (REQ-1405).** Les requêtes `Query{TypeName}GroupBy` peuvent définir `include_nodes: true` pour inclure les colonnes scalaires de la table de base dans le champ `nodes` de chaque ligne. Le champ de chaîne répétée `include` nomme les champs de relation plusieurs-à-un dont les colonnes scalaires sont aussi imbriquées dans `nodes`. Cela correspond au comportement JSON:API `?includeNodes=` / `?include=`. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Pilote JDBC

Le pilote JDBC Provisa (`provisa-jdbc-0.1.0.jar`) expose le catalogue sémantique aux outils BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL de connexion :** `jdbc:provisa://host:port` (REQ-131)

Les domaines sont mappés en schémas JDBC. (REQ-127) Les tables utilisent leurs alias enregistrés. Les colonnes utilisent des alias et font apparaître les descriptions comme `REMARKS`. (REQ-128) Les méthodes de métadonnées standards (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exposent les relations sémantiques comme métadonnées PK/FK.

**Prise en charge SQL :** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Le pilote demande une redirection Arrow IPC par défaut. Les résultats circulent lot par lot via `ArrowStreamReader`, limités à un lot d'enregistrements en mémoire. (REQ-293)

---

## Format de l'argument `orderBy`

L'argument `order_by` utilise des objets `{column: direction}` avec une énumération de direction à 6 valeurs : (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Directions prises en charge : `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Abonnements

Les abonnements SSE sont disponibles à `GET /data/subscribe/{table}`. (REQ-219, REQ-258) La livraison des notifications utilise un fournisseur enfichable sélectionné selon le type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY`, les sources MongoDB utilisent Change Streams, et les sources Kafka utilisent des groupes de consommateurs. Le filtrage par sécurité au niveau des lignes et la validation de schéma s'appliquent quel que soit le fournisseur. Les sources WebSocket et RSS sont également prises en charge via le même point de terminaison. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Glossaire métier (REQ-1387)

Le glossaire métier fait correspondre les noms de champs physiques — tels qu'ils existent dans les bases de données sources — à un vocabulaire humain partagé. Chaque colonne enregistrée dans la couche sémantique reçoit automatiquement un terme. Aucune saisie manuelle n'est requise pour peupler le glossaire ; les curateurs ajoutent des définitions, des relations et des experts par-dessus ce que le système dérive.

### Comment les termes sont dérivés

Lorsque Provisa enregistre ou met à jour les colonnes d'une table, `normalize_term` (`provisa/core/glossary.py`) s'exécute sur chaque nom de colonne et produit une expression canonique. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

La normalisation applique cinq règles en séquence :

1. Découper aux frontières camelCase et aux caractères séparateurs (`_`, `-`, `.`, `/`, espace).
2. Mettre le résultat en minuscules.
3. Développer une table d'abréviations fixe (p. ex. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Retirer un **jeton de substitution** final (`identifier`, `code`, `index`, ou `reference`) — une colonne nommée d'après sa clé ou son code pointe vers le concept sous-jacent via une valeur de substitution, donc le terme devrait être le concept lui-même. Le dernier jeton restant n'est jamais retiré.
5. Qualifier une **expression trop générique** avec le concept de la table. Lorsque l'expression normalisée complète est un simple mot d'attribut (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, et similaires), le terme devient `<concept de table> <expression>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Un seul terme `name` partagé entre des tables non liées fusionnerait des significations distinctes ; la qualification relie chaque colonne à son concept englobant à la place. Le concept de table est le nom métier de la table, normalisé avec un nom de tête singulier (`order_lines` → `order line`).

Les pseudo-colonnes de filtre natif (préfixées `_nf_`, ou toute colonne portant `native_filter_type`) sont de la mécanique de paramètres de requête, pas des champs métier, et ne dérivent aucun terme.

Parce que `id`, `key`, `pk`, et `sk` se développent tous en `identifier` avant la vérification du jeton de substitution, trois noms de colonnes physiquement différents aboutissent exactement au même terme :

| Nom physique | Après normalisation |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Les trois premiers se réduisent à un seul terme. `transaction amount` conserve les deux jetons parce que `amount` n'est pas un jeton de substitution. Une colonne `id` isolée — sans jeton précédent — ne peut pas être retirée ; elle se normalise en `identifier` afin que le terme ne soit pas vide. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Cycle de vie

Les termes sont **dérivés de l'appartenance à la couche sémantique**, pas créés à la demande par les utilisateurs. Le référentiel de table est le chemin d'écriture unique : `sync_table_refs` s'exécute à l'intérieur de chaque mise à jour d'ensemble de colonnes, et `sweep_refless_terms` s'exécute après tout chemin de suppression. [tool-verified: `provisa/core/repositories/glossary.py`]

**Lorsqu'une colonne est ajoutée :** Provisa recherche le terme normalisé par nom. S'il existe déjà, la colonne obtient une référence vers lui (et si le terme était déprécié, il est réactivé — `deprecated` est remis à `False`). Si aucun terme n'existe encore, un est créé.

**Lorsqu'une colonne disparaît** (changement de schéma ou suppression de table) : sa référence est supprimée et le terme est **réglé** selon une règle de suppression-ou-dépréciation. Un terme ancré sans référence restante est supprimé purement et simplement — avec ses arêtes et affectations d'experts — sauf si le supprimer laisserait un terme abstrait déconnecté de tous les termes ancrés (aucun chemin dans le graphe de termes). Dans ce cas, le terme est **déprécié** (marqué `deprecated=True`) plutôt que supprimé, afin que l'ancrage du graphe du terme abstrait survive.

Les termes abstraits ne sont jamais supprimés automatiquement ; ils existent en dehors du cycle de vie physique et ne sont supprimés qu'explicitement via l'API admin.

**Réactivation :** si le nom normalisé d'un terme déprécié réapparaît (une colonne est réenregistrée), le terme est démarqué et ses références recommencent à s'accumuler.

### Points de terminaison de curation

Tous les points de terminaison sont sous `/admin/glossary`. Ils requièrent un accès `org_admin` et une organisation configurée. Chaque mutation déclenche une publication de métadonnées. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Liste les termes. Paramètres de requête : `q` (recherche nom/définition), `include_deprecated` (par défaut `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Obtient le détail du terme : définition, références physiques, arêtes typées, experts |
| `POST` | `/admin/glossary/terms` | Crée un terme abstrait — vocabulaire utilisateur sans référence physique |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Renomme, définit une définition, ou bascule l'exclusion d'export |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Supprime un terme sans référence physique |
| `POST` | `/admin/glossary/refs/move` | Déplace une référence physique vers un autre terme (consolidation) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Ajoute une arête de relation typée entre deux termes |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Supprime une arête (paramètres de requête : `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Étiquette un utilisateur comme expert ou auteur pour un terme |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Supprime la désignation d'expert/auteur d'un utilisateur |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Rédige une définition pour un terme en utilisant le modèle IA de l'organisation — renvoie uniquement le texte, rien n'est persisté avant l'enregistrement |
| `POST` | `/admin/glossary/definitions/generate` | Génère et persiste des définitions pour chaque terme qui n'en a aucune — n'écrase jamais un texte rédigé par un humain |
| `POST` | `/admin/glossary/relationships/generate` | Propose et persiste des arêtes typées à travers tout le glossaire en utilisant le modèle IA de l'organisation |

**Corps de `POST /admin/glossary/terms` :**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Corps de `POST /admin/glossary/terms/{term_id}/edges` :**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Valeurs `rel_type` valides : `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Corps de `POST /admin/glossary/terms/{term_id}/experts` :**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valeurs `kind` valides : `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**Corps de `POST /admin/glossary/refs/move` :**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Déplacer une référence règle le terme perdant selon la règle de suppression-ou-dépréciation. Utilisez ceci pour consolider deux termes que la normalisation a gardés séparés — par exemple, après qu'une source utilise une abréviation non standard tombée en dehors de la table d'expansion.

Supprimer un terme ancré (avec des références physiques) renvoie `400 glossary.invalid`. Supprimez ou déplacez d'abord toutes les références.

**`PATCH /admin/glossary/terms/{term_id}` — champ `export_excluded` :**

```json
{"export_excluded": true}
```

Définir `export_excluded` à `true` retient le terme de tous les instantanés d'export de métadonnées, indépendamment de ses références physiques ou de son statut abstrait. Le remettre à `false` restaure le terme dans l'instantané à la prochaine publication. Les données de curation (définition, arêtes, experts) ne sont pas affectées. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Curation assistée par IA

Le modèle IA configuré de l'organisation peut rédiger des définitions et proposer des arêtes de relation à travers tout le glossaire en une seule opération. Les deux actions groupées requièrent un accès `org_admin` et une organisation configurée.

**`POST /admin/glossary/definitions/generate`**

Parcourt chaque terme du glossaire, ignore ceux qui ont déjà une définition, et appelle le modèle IA de l'organisation pour en rédiger une pour chaque terme restant. Le brouillon est persisté immédiatement — contrairement au point de terminaison de brouillon par terme (`POST /admin/glossary/terms/{term_id}/definition/generate`), il n'y a pas d'étape d'édition. Les définitions rédigées par des humains ne sont jamais écrasées : la garde est `if summary["definition"]: continue` avant tout appel au modèle. Une seule notification de publication couvre tout le lot. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Réponse :

```json
{"generated": 12}
```

`generated` est le nombre de termes ayant reçu une nouvelle définition. Il est de zéro lorsque chaque terme en a déjà une.

**`POST /admin/glossary/relationships/generate`**

Envoie la liste complète des termes au modèle IA de l'organisation avec une invite qui spécifie les dix types d'arêtes autorisés (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) et demande uniquement des propositions sûres. Le modèle répond avec un tableau JSON ; chaque entrée est validée avant toute écriture : les noms de termes inconnus, les auto-arêtes, et les types d'arête en dehors de l'énumération fermée sont silencieusement rejetés. Les propositions valides sont insérées de façon idempotente — réexécuter l'action ne duplique pas les arêtes. Une seule notification de publication couvre le lot. Le point de terminaison renvoie `{"added": 0}` immédiatement lorsque le glossaire contient moins de deux termes non dépréciés. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Réponse :

```json
{"added": 5}
```

`added` est le nombre d'arêtes écrites. Une arête déjà existante compte quand même — l'insertion réussit, mais les données de l'arête ne changent pas.

### Outil MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Recherche les noms et définitions de termes avec une correspondance de sous-chaîne insensible à la casse, jusqu'à `limit` résultats. Chaque résultat est le détail complet du terme : `name`, `definition`, `is_abstract`, `deprecated`, références physiques (avec `source_id`, `schema_name`, `table_name`, `column_name`), arêtes typées, et affectations d'experts. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Utilisez `search_terms` avant d'écrire du SQL pour trouver chaque champ physique qui représente un concept par nom. Par exemple, rechercher `"order date"` renvoie le terme et toutes les colonnes `order_dt`, `orderDate`, `ORDER_DATE` à travers chaque table enregistrée.

### Export de métadonnées

Le graphe de termes du glossaire est inclus dans chaque `MetadataSnapshot` construit par `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

L'export applique les mêmes filtres que le reste de l'instantané :

- Un terme marqué `export_excluded` est retenu purement — indépendamment de ses références physiques, de son statut abstrait, ou du fait que le catalogue de l'organisation soit configuré. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Un terme ancré ne publie que lorsqu'au moins une de ses références physiques appartient à une colonne qui passe à la fois le filtre **Produit de données** (le drapeau `data_product` de la table doit être `true`) et le filtre de colonne **technique** (les colonnes étiquetées `technical` sont retenues).
- Un terme ancré dont toutes les références sont retenues par ces filtres est retenu avec elles.
- Les termes abstraits publient sans condition — ce sont du vocabulaire utilisateur, non lié à des colonnes physiques.
- Une arête entre deux termes ne publie que lorsque les deux termes aux extrémités publient.

Chaque adaptateur fournisseur publie le graphe de termes nativement, dans un conteneur de glossaire propriété de Provisa qu'il crée de façon idempotente — jamais dans un glossaire de catalogue existant :

| Fournisseur | Conteneur | Termes | Relations | Dépréciation |
| --- | --- | --- | --- | --- |
| Apache Atlas | « Provisa Glossary » (API glossaire) | termes de glossaire, définition sur `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | marqueur shortDescription `[DEPRECATED]` |
| Atlan | Glossaire Provisa par qualifiedName stable | `longDescription` (jamais le `userDescription` édité par l'humain) | même mappage Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | aspect `glossaryTermInfo` par terme | KIND_OF → Inherits, PART_OF → Contains (inversé), RELATED_TO/SYNONYM_OF → termes associés | aspect de dépréciation ; les renommages suivent la succession d'URN |
| OpenMetadata | Glossaire Provisa via `/v1/glossaries` | PUT indexé par fqn, renommages PATCH-rebind par UUID stocké | KIND_OF → hiérarchie parente native, SYNONYM_OF → `synonyms`, autres → `relatedTerms` | `entityStatus` |
| Collibra | Domaine de type glossaire « Provisa Glossary » | Actifs Business Term via l'API Import | types de relation Business Term natifs | statut d'actif |

La propriété est le lien contraignant, pas le nom : l'id fournisseur de chaque terme publié est capturé dans `catalog_bindings` sous l'URN du terme (`provisa://<org>/terms/<name>`), et Provisa ne modifie ou ne supprime un élément de glossaire côté fournisseur que lorsqu'il détient ce lien (ou que l'élément vit dans le conteneur propriété de Provisa qu'il a créé). Un élément de glossaire sans lien Provisa provient du système externe et n'est jamais touché ; les mises à jour lisent-fusionnent afin que les champs ajoutés par le steward sur les propres termes de Provisa survivent ; rien n'est supprimé lorsqu'un terme quitte l'instantané. Les affectations terme-vers-actif du steward restent la propriété de l'externe — aucun adaptateur n'écrit d'affectations terme-vers-actif (la publication des affectations rédigées par Provisa est un suivi explicite). Sur Collibra spécifiquement, la sécurité sous la sémantique REPLACE de l'API Import repose sur le confinement : la charge utile ne mentionne que des actifs à l'intérieur du domaine de glossaire Provisa et des instances de relation uniquement entre termes Provisa, de sorte que les glossaires du steward et leurs relations ne sont jamais atteignables. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]

---

## Produits de données (REQ-1634)

Un produit de données regroupe des tables publiées ensemble pour la consommation, possédées par exactement un domaine. Les champs suivent le vocabulaire ODPS (Open Data Product Standard) là où Provisa possède déjà la source de vérité. L'interface admin expose les produits de données sous **Admin → Data Products**. [tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/schema_mutation.py:949-1017`, `provisa/api/admin/schema_query.py:352-362`]

### Capacités

| Capacité | Accorde |
| --- | --- |
| `data_product_read` | Accès en lecture au champ de requête `data_products` et à la page admin Produits de données. Semé par défaut à `org_admin`, `analyst`, `developer`, et `modeler`. |
| `data_product_rw` | Mutations de création et de suppression. Active les contrôles Nouveau / Modifier / Supprimer dans l'interface. |

[tool-verified: `provisa/api/admin/schema_mutation.py:959,1001`, `provisa/api/admin/schema_query.py:357`]

### GraphQL admin

Toutes les opérations sur les produits de données passent par `POST /admin/graphql`.

**Requête :**

```graphql
query {
  data_products {
    id
    domain_id
    name
    owner_role
    team_role
    purpose
    limitations
    usage
    version
    status
    sla
    support
    custom_properties
  }
}
```

Requiert `data_product_read`.

**Créer ou mettre à jour :**

```graphql
mutation {
  create_data_product(input: {
    id: "customer_360"
    domain_id: "sales"
    name: "Customer 360"
    owner_role: "data-product-owner"
    team_role: "sales-analytics"
    purpose: "Single view of a customer across all touchpoints."
    status: "active"
    version: "1.0.0"
  }) {
    success
    message
  }
}
```

`create_data_product` fait un upsert — l'appeler avec un `id` existant met à jour l'enregistrement. Requiert `data_product_rw`.

**Supprimer :**

```graphql
mutation {
  delete_data_product(id: "customer_360") {
    success
    message
  }
}
```

Supprimer un produit efface `product_id` de chaque table membre, supprimant leur appartenance. Requiert `data_product_rw`. [tool-verified: `provisa/api/admin/schema_mutation.py:995-1017`]

### Schéma de champs

| Champ | Type | Requis | Notes |
| --- | --- | --- | --- |
| `id` | `String` | Oui | Identifiant stable lisible par machine, p. ex. `customer_360` |
| `domain_id` | `String` | Oui | Domaine propriétaire. Les tables membres doivent partager ce `domain_id` — les incohérences sont rejetées à l'enregistrement |
| `name` | `String` | Oui | Nom d'affichage |
| `owner_role` | `String` | Non | Rôle responsable de ce produit ; distinct du steward de domaine |
| `team_role` | `String` | Non | Rôle dont les titulaires assurent la maintenance quotidienne de ce produit ; résolu en individus |
| `purpose` | `String` | Non | Ce que ce produit publie et pourquoi |
| `limitations` | `String` | Non | Contraintes, mises en garde ou exclusions connues |
| `usage` | `String` | Non | Comment consommer ce produit |
| `version` | `String` | Non | p. ex. `1.2.0` |
| `status` | `String` | Non | p. ex. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | `String` | Non | Engagements de niveau de service ; texte libre — un produit s'étend sur plusieurs tables et un SLA structuré ne peut nommer sans ambiguïté quel membre il décrit |
| `support` | `String` | Non | Indications de support en texte libre |
| `custom_properties` | `JSON` | Non | Métadonnées clé-valeur arbitraires non couvertes par les champs standards |

Deux champs supplémentaires existent sur le modèle mais ne sont pas exposés dans le `DataProductType` / `DataProductInput` Strawberry — ils sont spécifiques à Snowflake Horizon Catalog (REQ-1635) :

| Champ | Notes |
| --- | --- |
| `support_contact` | E-mail ou URL ; requis par les manifestes de fiche d'organisation Horizon Catalog |
| `publish` | `true` pour publier immédiatement les fiches Horizon ; les nouvelles fiches sont en DRAFT par défaut |

[tool-verified: `provisa/core/models.py:338-341`, `provisa/api/admin/types.py:104-118,538-551`]

### Appartenance de table

Une table rejoint un produit de données en définissant son champ `product_id` au formulaire d'édition de table. Le sélecteur est délimité aux produits dont le `domain_id` correspond au domaine propre de la table — une table du domaine `marketing` n'est jamais proposée un produit du domaine `sales`. [tool-verified: `provisa/api/admin/actions_router.py:244-260`, `docs/arch/requirements.yaml:54585-54586`]

Les commandes du même domaine peuvent aussi être affectées comme membres. [tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:commandsLabel`]

### Filtre d'export de métadonnées

`build_snapshot` applique `data_products_only=True` pour chaque publication de catalogue. Les tables sans `product_id` sont retenues de l'instantané, ainsi que leurs arêtes de relation, arêtes de traçabilité, et étiquettes de gouvernance. Les sources et domaines sont toujours publiés. Les termes du glossaire ne publient que lorsqu'au moins une de leurs références physiques appartient à une table exportée (membre d'un produit). [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Un produit sans membre exporté ne construit pas d'entrée d'instantané — une fiche sans membre représenterait mal le produit auprès du catalogue. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

### Prise en charge des produits de données par cible de catalogue

`MetadataSnapshot.data_products` atteint chaque adaptateur, mais seuls les adaptateurs dont la plateforme a un concept natif de produit de données le publient comme entité de premier ordre ; les autres publient les tables membres (déjà filtrées ci-dessus) sans regroupement en produit.

| Cible | Représentation du produit de données |
| --- | --- |
| Snowflake Horizon | Chaque produit devient un `SHARE` sur les adresses physiques de ses tables membres, enveloppé dans un `CREATE ORGANIZATION LISTING` interne — un produit de données natif Horizon Catalog. `publish=true` publie la fiche en direct immédiatement ; sinon elle atterrit en DRAFT. [tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:21-34,389-418`] |
| BigQuery Dataplex | Chaque produit devient une fiche Analytics Hub via `/v1/dataProducts`. [tool-verified: `provisa/api/metadata_export/bigquery_dataplex.py:100,136,159`] |
| OpenMetadata | Chaque produit devient une entité `DataProduct` native (`/api/v1/dataProducts`), avec une propriété dérivée du domaine. [tool-verified: `provisa/api/metadata_export/openmetadata.py:326-344,635`] |
| DataHub | Chaque produit devient une entité `dataProduct` native (`urn:li:dataProduct:...`) avec ses propres aspects `dataProductProperties`/propriété. [tool-verified: `provisa/api/metadata_export/datahub.py:133-136,443-483`] |
| Collibra | Chaque produit devient un actif d'un type de communauté `Data Product`, lié à ses tables membres via une relation `Data Product groups Table`. [tool-verified: `provisa/api/metadata_export/collibra.py:129-133,371-388`] |
| Atlan | Publié comme une estimation de typedef personnalisé `DataProduct` — Atlan n'a pas de nom de type stable documenté pour ce concept, donc le mappage est au mieux. [tool-verified: `provisa/api/metadata_export/atlan.py:60`] |
| Apache Atlas | Publié comme un typedef personnalisé `provisa_data_product` avec une relation `provisa_data_product_members` — Atlas n'a pas de type d'entité de produit de données natif. [tool-verified: `provisa/api/metadata_export/atlas.py:134-147,191,256-260`] |
| OpenLineage | Pas une entité de premier ordre — les tables membres portent une facette personnalisée `provisa_data_product` nommant le produit propriétaire. [tool-verified: `provisa/api/metadata_export/openlineage.py:243,348`] |
