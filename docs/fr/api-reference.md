# Référence API {: #api-reference }

## Vue d'ensemble {: #overview }

Provisa expose des points de terminaison REST sous deux préfixes : `/data` pour l'exécution de requêtes et l'introspection de schéma, et `/admin` pour la gestion de la configuration. La plupart des points de terminaison de données requièrent un identifiant de rôle. Les opérations de configuration admin utilisent une API GraphQL Strawberry à `/admin/graphql`.

---

## Authentification {: #authentication }

Lorsque `auth.provider` est configuré dans `provisa.yaml`, tous les points de terminaison à l'exception de `/health` et `/setup/status` requièrent un en-tête `Authorization: Bearer <token>`.

Sans authentification configurée, le serveur fonctionne en mode développement. Toute requête est traitée comme l'identité `anonymous`, qui correspond à tous les rôles configurés avec un accès domaine générique.

**Connexion (`POST /auth/login`)** est fournie par le fournisseur d'authentification actif lorsque `provider: basic` est configuré. Le format des identifiants et la réponse dépendent du fournisseur.

**Introspection d'identité :**

```http
GET /auth/me
```

Renvoie l'id, l'e-mail, le nom d'affichage, les appartenances aux organisations et les affectations de rôle de l'utilisateur authentifié. En mode développement, renvoie `dev_mode: true` avec la liste de tous les id de rôle.

```http
GET /auth/provider-type
```

Renvoie `{"provider": "<name>"}` ou `{"provider": null}` lorsque l'authentification n'est pas configurée.

---

## Points de terminaison de données {: #data-endpoints }

### `POST /data/graphql`

Exécute une requête ou une mutation GraphQL.

**Corps de la requête :**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "org_admin",
  "extensions": {}
}
```

La requête s'exécute sous son rôle agissant : le rôle de l'utilisateur authentifié ou, en mode développement (sans authentification), l'en-tête `X-Provisa-Role`, `org_admin` en l'absence de l'en-tête. Le champ `role` du corps ne sélectionne pas de rôle. S'il est présent, il doit égaler le rôle agissant ; une valeur différente renvoie `400` avec le code `data.role_mismatch`, en nommant les deux rôles. La même règle s'applique à `POST /data/sql`, `POST /data/query` et `POST /query/nl`.

Le champ `extensions` prend en charge le protocole Automatic Persisted Query (APQ) :

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**En-têtes :**

- `X-Provisa-Role` — le rôle sous lequel s'exécuter (mode développement) ; avec l'authentification, une demande de rôle honorée seulement si le rôle est attribué à l'utilisateur
- `Accept` — format de réponse (voir Négociation de contenu)
- `Authorization` — `Bearer <token>` lorsque l'authentification est activée
- `X-Provisa-Redirect-Format` — type MIME pour la sortie de redirection S3
- `X-Provisa-Redirect-Threshold` — nombre de lignes au-delà duquel la redirection se déclenche
- `X-Provisa-Redirect` — `true` pour forcer la redirection sans condition

**Cache de réponses :** activation par requête avec la directive d'opération `@cached` (`query @cached(ttl: 60) { ... }`, `ttl` facultatif) ; la même directive s'applique au GraphQL envoyé via Arrow Flight. Cypher s'active avec une ligne `// @provisa cache=true` ou `// @provisa cache_ttl=N`, gRPC avec les métadonnées d'appel `x-provisa-cache: true` ou `x-provisa-cache-ttl: N`. Sans elle, la requête ne lit ni n'écrit le cache de réponses. L'en-tête de réponse `X-Provisa-Cache` indique `HIT` ou `MISS`. Voir [Cache](configuration.md#per-request-opt-in).

<a id="response-cache-request-headers"></a>

#### En-têtes de requête du cache de réponses {: #response-cache-request-headers }

`/data/graphql`, `/data/cypher`, la Neo4j Query API (`/db/{database}/query/v2`), `/data/rest` et `/data/jsonapi` acceptent aussi l'activation sous forme d'en-tête de requête. Chaque forme ci-dessous se comporte comme `@cached(ttl: N)` :

| En-tête | Équivalent à |
| --- | --- |
| `Cache-Control: max-age=N` / `Cache-Control: max-stale=N` | `@cached(ttl: N)` |
| `Cache-Control: max-stale` (sans valeur) | `@cached` (le TTL de l'opérateur) |
| `Cache-Control: no-cache` / `no-store` | pas de cache, comme si rien n'était envoyé |
| `x-provisa-cache: true` | `@cached` |
| `x-provisa-cache-ttl: N` | `@cached(ttl: N)` |

Une requête sans l'un de ces en-têtes et sans indication dans son texte n'est pas mise en cache, alors que les caches HTTP simples le font par défaut. Les paramètres de cache de l'opérateur s'appliquent toujours comme pour `@cached` : un en-tête ne rend jamais une lecture plus récente qu'ils ne le permettent, et n'est jamais refusé parce qu'il demande un `ttl` plus long. Les autres directives `Cache-Control` (`no-transform`, `min-fresh`, ...) sont ignorées, comme la RFC 9111 l'exige d'un cache.

Une requête peut indiquer l'activation plusieurs fois : dans son texte et dans un en-tête, ou dans deux en-têtes. Si elles concordent, la requête est mise en cache une fois comme demandé. Si elles divergent (par exemple `@cached(ttl: 60)` avec `Cache-Control: max-age=5`, ou `x-provisa-cache-ttl: 10` avec `Cache-Control: max-age=20`), la requête échoue avec `400` et le code `data.invalid_cache_control`, en nommant chaque forme envoyée. Une valeur `N` qui n'est pas un nombre entier de secondes échoue de la même façon. Aucune forme n'est préférée à l'autre.

```bash
curl -H 'Cache-Control: max-age=120' -H 'X-Provisa-Role: analyst' \
  'http://localhost:8001/data/rest/sales/orders?limit=10'
```

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

Les requêtes à racines multiples exécutent chaque champ racine indépendamment. Les champs en dessous du seuil de redirection sont retournés en ligne ; ceux au-dessus sont redirigés. La clé `redirects` (au pluriel) associe les noms de champs aux informations de redirection.

**En-têtes de cache :**

- `X-Provisa-Cache: HIT|MISS`
- `X-Provisa-Cache-Age: <seconds>` (sur HIT)

**Capacités requises :** `QUERY_DEVELOPMENT` pour toutes les requêtes, y compris l'introspection.

---

### Négociation de contenu {: #content-negotiation }

| En-tête Accept | Format |
| --- | --- |
| `application/json` | JSON (par défaut) |
| `application/x-ndjson` | JSON délimité par des retours à la ligne |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

---

### Redirection {: #redirect }

Les résultats dépassant un seuil de lignes configuré (ou lorsque `X-Provisa-Redirect: true`) sont écrits sur S3 et une URL pré-signée est renvoyée.

| Format de redirection | Écrit par | Mémoire |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS fédéré | Aucune — les données ne transitent jamais par Provisa |
| `application/x-orc` | CTAS fédéré | Aucune — les données ne transitent jamais par Provisa |
| `application/json` | Provisa | Limité par la mémoire |
| `application/x-ndjson` | Provisa | Limité par la mémoire |
| `text/csv` | Provisa | Limité par la mémoire |
| `application/vnd.apache.arrow.stream` | Provisa | Limité par la mémoire |

Pour les exports analytiques volumineux, utilisez la redirection Parquet ou ORC. Le moteur de fédération écrit directement sur S3 en parallèle — aucune donnée ne transite par Provisa.

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Exécute du SQL brut à travers le pipeline de gouvernance de l'étape 2.

**Corps de la requête :**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "org_admin"
}
```

`params` (facultatif) porte les valeurs des espaces réservés `$1…$n` de l'instruction, dans l'ordre, séparément de son texte :

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = $1 AND amount > $2",
  "params": ["us", 100]
}
```

Les valeurs sont liées à l'exécution de l'instruction — par le moteur ou par le pilote de la source — et ne sont jamais écrites dans l'instruction, de sorte qu'une valeur ne peut pas changer ce qu'est l'instruction. Elles accompagnent exactement une instruction dont les espaces réservés les numérotent exactement : un lot d'instructions, un espace réservé sans valeur ou une valeur qu'aucun espace réservé ne prend est refusé avec `400` et le code `data.sql_parameters_do_not_fit` avant que l'instruction ne soit gouvernée ou exécutée. Une instruction sans espace réservé s'envoie sans `params`, comme avant.

**Capacités requises :** `QUERY_DEVELOPMENT`.

Les violations de gouvernance sur `POST /data/sql` renvoient un statut HTTP 403.

**Réponse :** même format que `/data/graphql` (lignes JSON par défaut, négociées via `Accept`).

---

### `POST /data/query`

Point de terminaison de requête unifié. Accepte GraphQL, SQL ou Cypher — la syntaxe est détectée automatiquement.

Les requêtes Cypher peuvent aussi être soumises au point de terminaison dédié `POST /query/cypher`.

**Corps de la requête :**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "org_admin"
}
```

Renvoie `{"data": ...}` pour GraphQL, `{"columns": [...], "rows": [...]}` pour SQL et Cypher.

---

### `POST /data/sql/explain`

Explique ou analyse une instruction SQL à travers le pipeline gouverné.

Le point de terminaison enveloppe le SQL **gouverné** — l'instruction qui s'exécute réellement sous le rôle de l'appelant, après la sécurité au niveau des lignes et le masquage — dans la syntaxe EXPLAIN du dialecte. Ce que montre le plan est la version autorisée de la requête, pas l'entrée brute.

**Corps de la requête :**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "org_admin",
  "analyze": false
}
```

Définissez `analyze: true` pour exécuter EXPLAIN ANALYZE. La requête s'exécute et le plan porte des nombres de lignes et des durées réels. Tous les dialectes ne prennent pas en charge ANALYZE ; voir le tableau dans [Plans de requête et statistiques](engines.md#query-plans-and-statistics).

**Réponse :** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` lorsque le dialecte ne prend pas en charge EXPLAIN, ou lorsque `analyze: true` est demandé sur un dialecte qui ne le prend pas en charge (p. ex. SQLite).

---

### `GET /data/engine/state`

Renvoie l'état actuel du shard moteur sans le réveiller.

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

---

### `POST /data/engine/prewarm`

Déclenche un réveil du moteur sans exécuter de requête.

Renvoie `202 Accepted` immédiatement. Le réveil s'exécute en arrière-plan. Utilisez ceci si vous voulez que le moteur soit prêt avant l'arrivée de la première requête — par exemple, depuis un planificateur qui exécute des requêtes quelques minutes plus tard.

**Réponse :** `202 Accepted`, corps `{"started": true}`

---

### `GET /data/rest/{domain_id}/{table_name}`

Point de terminaison REST simple auto-généré pour chaque table enregistrée. La chaîne de requête est mappée aux arguments GraphQL et la requête est compilée et exécutée via le même pipeline (sécurité au niveau des lignes, masquage, routage) que GraphQL.

**Paramètres de requête :**

- `limit` — nombre maximal de lignes (≥ 1)
- `offset` — nombre de lignes à ignorer (≥ 0)
- `fields` — noms de champ séparés par des virgules, tels que les écrit le schéma GraphQL, en camelCase avec la convention de nommage par défaut, par ex. `?fields=orderId,amount` (par défaut, tous les champs scalaires). Les noms de champ de `filter` et `orderBy` suivent la même écriture. JSON:API prend à la place le nom physique de la colonne (`fields[orders]=order_id`).
- `filter` — tableau JSON d'objets de filtre `{"field", "comparator", "value"}`. Une valeur est lue selon le type de sa colonne : `"100"` sur une colonne numérique se compare comme 100 ; une valeur que le type de la colonne ne peut contenir renvoie `400`.
- `orderBy` — tableau JSON d'objets de tri `{"field", "direction"}`
- `params` — les paramètres de la table en un objet JSON par nom de paramètre, p. ex. `?params={"region":"EU"}`. Une table enregistrée sur une API, ou une table de requête, a un paramètre pour chaque entrée que prend sa source. Une lecture sans un paramètre obligatoire renvoie `400` en le nommant, de même qu'un nom pour lequel la table n'a pas de paramètre. Chaque valeur est vérifiée selon le type de son paramètre.

Les valeurs de filtre et de paramètre voyagent comme variables à côté du texte GraphQL que construit l'endpoint : une valeur contenant un guillemet est comparée telle qu'écrite.

Le rôle authentifié est requis ; les requêtes non authentifiées renvoient `401`. Une spécification OpenAPI pour ces routes est servie à `GET /data/rest/openapi.json` avec l'interface Swagger UI à `GET /data/rest/docs`.

**Cache de réponses :** activation par un en-tête de requête, `Cache-Control: max-age=N` ou `x-provisa-cache: true` (voir [En-têtes de requête du cache de réponses](#response-cache-request-headers)). Chaque réponse porte `X-Provisa-Cache: HIT|MISS`, et `X-Provisa-Cache-Age` en cas de succès.

#### Explorateur OpenAPI / Swagger UI {: #openapi-swagger-ui-explorer }

La page de l'explorateur OpenAPI (`/app/openapi`) intègre Swagger UI dans un iframe en bac à sable. La spécification est délimitée par rôle — seules les tables et colonnes visibles pour le rôle actuel apparaissent — et optionnellement filtrée par domaine via le sélecteur de domaine. L'interface bascule automatiquement entre les thèmes clair et sombre.

La page charge le HTML de la spécification via `fetch()` plutôt qu'un `src` d'iframe direct, de sorte que la requête porte le jeton porteur de la session et que les requêtes relatives propres à Swagger UI se résolvent correctement contre la même origine.

Lorsqu'elle est atteinte depuis un lien NL « Ouvrir dans OpenAPI », la page développe automatiquement le point de terminaison ciblé, renseigne les paramètres de requête à partir de l'URL générée par NL (p. ex. `aggregate`, `groupBy`), et clique sur Exécuter — en utilisant un sondage du DOM pour s'assurer que chaque étape se termine avant que la suivante ne se déclenche.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Point de terminaison conforme à [JSON:API](https://jsonapi.org) auto-généré pour chaque table enregistrée. Même sécurité au niveau des lignes, masquage et routage que GraphQL.

**En-tête `Accept` :** doit inclure `application/vnd.api+json` (le type MIME JSON:API) sinon la requête renvoie `406`.

**Paramètres de requête :**

- `fields[<type>]` — jeux de champs clairsemés, par ex. `?fields[orders]=amount`. Les noms sont les noms physiques des colonnes, par ex. `fields[orders]=order_id`, comme pour `filter[...]`, `sort` et les clés d'attribut émises. REST prend à la place l'écriture GraphQL (`fields=orderId`).
- `filter[<col>]` / `filter[<col>][<op>]` — p. ex. `?filter[region]=US`, `?filter[amount][gt]=100`. Une valeur est lue selon le type de sa colonne ; une valeur que le type ne peut contenir renvoie `400`.
- `params` — les paramètres de la table en un objet JSON par nom de paramètre, p. ex. `?params={"region":"EU"}`, comme sur l'endpoint REST. Un paramètre obligatoire manquant renvoie `400` en le nommant ; un nom inconnu ou une valeur de mauvais type renvoie `400` avec `source.parameter` égal à `params`. Les liens de pagination conservent le paramètre.
- `sort` — séparé par des virgules, préfixe `-` pour l'ordre décroissant, p. ex. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — pagination
- `page[total]` — `true` compte les lignes qui correspondent au filtre et renvoie `meta.total` et `links.last`. Sans lui, la réponse ne porte que `links.self`, `links.first`, `links.prev` et `links.next`, et la base de données n'exécute aucun comptage. Toute valeur autre que `true` ou `false` renvoie `400`. Les liens d'une réponse conservent le paramètre.
- `aggregate` — fonctions d'agrégation séparées par des virgules à exécuter à la place de la récupération de lignes : `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Utilisez `?aggregate=count,sum` pour demander un sous-ensemble. Les réponses d'agrégation renvoient `data: null` avec les résultats dans `meta.aggregate`.
- `groupBy` — noms de colonnes séparés par des virgules ; utilisé avec `?aggregate=` pour regrouper les résultats. Seules les colonnes dans l'énumération `DistinctOnColumn` de la table sont valides ; le serveur renvoie `400` pour toute colonne que le rôle ne peut pas voir.
- `includeNodes` — `true` pour inclure les colonnes scalaires de la table de base (et les scalaires de dimension jointe nommés dans `include=`) à l'intérieur du tableau `nodes` de chaque ligne de groupe. Requis lorsqu'une requête de regroupement NL demande aussi des détails de dimension.

- `include` — noms de relation séparés par des virgules, en graphie physique, p. ex. `?include=order_items,shipment` : chaque ressource liée est renvoyée une fois dans `included`, et chaque ressource principale relie ce qu'elle inclut sous `relationships.<name>.data` (une liste d'identifiants de ressource pour une relation à plusieurs, un identifiant ou `null` pour une relation à un).

Les réponses sont des objets ressource avec `type`/`id`/`attributes`. L'`id` d'une ressource est la clé primaire de sa table : la valeur de la colonne clé ou, pour une clé composée, les valeurs de ses colonnes dans l'ordre de la clé jointes par `:` (la ligne 2 de la commande 7 d'une table de clé `(order_id, line)` est `7:2`). Un `:` ou un `\` dans une valeur est échappé par `\`. Une table sans clé primaire déclarée utilise sa colonne `id` ; une table sans l'une ni l'autre renvoie `400`, tout comme un `include` qui en atteint une. Les erreurs suivent la forme de l'objet d'erreur JSON:API.

**Cache de réponses :** activation par un en-tête de requête, `Cache-Control: max-age=N` ou `x-provisa-cache: true` (voir [En-têtes de requête du cache de réponses](#response-cache-request-headers)). Chaque réponse porte `X-Provisa-Cache: HIT|MISS`, et `X-Provisa-Cache-Age` en cas de succès. Deux formes divergentes renvoient `400` sous forme d'objet d'erreur JSON:API.

#### Explorateur JSON:API {: #jsonapi-explorer }

La page de l'explorateur JSON:API (`/app/jsonapi`) est une interface navigateur sur ces points de terminaison. Sélectionnez une table dans la liste groupée par domaine, puis configurez :

- **Champs** — choisissez les colonnes à inclure (jeu de champs partiel) ; ne rien cocher pour demander chaque colonne
- **Relations** — sélectionnez les noms de relation dérivés de clé étrangère à charger via `?include=`
- **Filtre** — champ, opérateur (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) et valeur
- **Tri** — un champ, ascendant ou descendant
- **Agrégation** — choisissez les colonnes de regroupement dans la liste validée par le serveur, puis cochez une ou plusieurs fonctions d'agrégation ; lorsque des colonnes de regroupement sont sélectionnées, une case « Inclure les nœuds » ajoute les colonnes scalaires de la table de base à chaque ligne
- **Taille de page** — ressources par page, avec navigation première/précédente/suivante/dernière

Les résultats s'affichent dans une vue résumée mise en forme (fiches ressources avec ancres de relation cliquables) ou un onglet JSON brut. L'URL de la requête en direct est affichée et peut être copiée. La sélection de table et la taille de page persistent d'une session à l'autre dans `localStorage`.

Lorsqu'elle est atteinte depuis un lien NL « Ouvrir dans JSON:API », l'explorateur présélectionne la table et amorce le sélecteur d'agrégation à partir des paramètres de requête générés par NL, puis exécute automatiquement la requête.

---

### `POST /query/nl`

Soumet une question en langage naturel. Le service démarre une tâche asynchrone et renvoie `202 Accepted` avec un `job_id` immédiatement. Requiert un fournisseur LLM configuré sous la section de configuration `ai_models`.

**Corps de la requête :**

```json
{"q": "How many orders were placed last month?", "role": "org_admin"}
```

Renvoie `{"job_id": "<id>"}`. Dépasser la limite de débit NL par rôle renvoie `429` avec un en-tête `Retry-After`.

**Récupérer le résultat :**

- `GET /query/nl/{job_id}` — interrogation. Renvoie le document de la tâche.
- `GET /query/nl/{job_id}/stream` — SSE. Un événement `branch` par cible de génération à mesure qu'elle se termine, puis un événement `done`.

Trois boucles de génération (Cypher, GraphQL, SQL) s'exécutent en parallèle, chacune validée par le compilateur et affinée en cas d'erreur. L'invite est délimitée au schéma visible du rôle. Le document de résultat classe chaque branche par cible :

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

Une branche qui épuise sa limite d'itérations renvoie `query: null`, `result: null`, et une chaîne `error`. Chaque requête générée s'exécute sous les droits du consommateur avec la gouvernance de l'étape 2 appliquée — le service ne contourne jamais la gouvernance.

#### Regroupement NL avec détails de dimension {: #nl-group-by-with-dimension-details }

Lorsqu'une requête de regroupement NL projette aussi des colonnes d'une table de dimension jointe — par exemple, « nombre de demandes par utilisateur avec nom et e-mail de l'utilisateur » — l'exécuteur dérive des chemins pointés par champ (`dim_paths`) à partir des colonnes de dimension projetées dans le SELECT. Ces chemins alimentent le paramètre `includeNodes=` sur les URL générées par les panneaux JSON:API et OpenAPI, de sorte que ces panneaux demandent les mêmes champs de dimension jointe que les branches SQL et GraphQL ont résolus. Sans cela, `includeNodes=true` ne renverrait que les champs scalaires propres de la table d'agrégation de base.

Sur le panneau gRPC, le `{Type}GroupByRequest` généré porte `include_nodes` (booléen) et `include` (chaîne répétée de noms de champs de relation). Le `{Type}GroupByRow` renvoyé inclut un champ `nodes` typé avec les lignes de détail de dimension.

---

### `GET /data/sdl`

Renvoie le SDL GraphQL pour le schéma d'un rôle.

**En-têtes :** `X-Provisa-Role: <role_id>` (requis)

**Paramètres de requête :**

- `domain` — id de domaine séparés par des virgules. Lorsque défini, la réponse est filtrée au(x) domaine(s) nommé(s) et aux tables accessibles depuis eux.

**Réponse :** SDL GraphQL en `text/plain`.

---

### `GET /data/introspection`

Renvoie le JSON d'introspection GraphQL, optionnellement filtré par domaine.

**En-têtes :** `X-Provisa-Role: <role_id>` (requis)

**Paramètres de requête :** `domain` — id de domaine séparés par des virgules.

**Réponse :** résultat d'introspection en `application/json`.

---

### `GET /data/graph-schema`

Renvoie la vue en graphe du schéma du rôle : étiquettes de nœud et leurs types de relation, pour les clients Cypher/graphe. Inclut `pk_columns` par étiquette de nœud afin que les appelants puissent déterminer les colonnes de clé primaire.

**Réponse :** `application/json` avec `node_labels` (chacune portant `pk`/`pk_columns`) et `relationship_types`.

Chaque type de relation porte aussi `junction_table_name` et `properties`. Sur une arête reposant sur une table de jonction, le premier nomme la table associative qu'elle parcourt et le second liste les colonnes de cette table lisibles comme `r.attr` et filtrables dans `WHERE` ; sur une arête reposant sur une clé étrangère, le nom est `null` et la liste de propriétés est vide, ce qui permet à un client de distinguer les deux cas. La table de jonction elle-même n'est jamais une étiquette de nœud — elle est l'arête, elle n'a donc aucune pastille dans un client de graphe ni aucune ligne dans `node_labels`.

---

### `GET /data/domains`

Renvoie les id de domaine accessibles au rôle demandeur.

**En-têtes :** `X-Provisa-Role: <role_id>` (requis)

**Réponse :** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Renvoie la chaîne de version de schéma actuelle. Combine un nonce par démarrage avec un compteur de reconstruction. Les clients utilisent ceci pour invalider les caches de schéma après les redémarrages du serveur.

**Réponse :** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/catalog`

Renvoie le catalogue du rôle sous lequel s'exécute la requête : les tables servies à ce rôle, telles que les donne la liste Arrow Flight.

**Réponse :** `{"tables": [{"path": ["<domain>", "<table>"], "schema": "<base64>"}]}`. `schema` est le schéma Arrow de la table dans l'encodage Arrow IPC : chaque colonne sous le nom par lequel SQL la lit, avec son type enregistré, sa description et ses métadonnées de clé (`primary_key`, `references`) dans les métadonnées du champ. Les commandes et les métriques ne sont pas listées.

Le rôle est celui qu'a établi la couche d'authentification (`X-Provisa-Role` peut demander un rôle détenu, ou un ensemble séparé par des virgules). Un déploiement sans fournisseur d'authentification liste le catalogue entier, comme la liste Flight. Le pilote JDBC lit cette route lorsque le port Flight est injoignable.

---

### `GET /data/proto/{role_id}`

Renvoie le fichier `.proto` auto-généré pour un rôle.

**Réponse :** schéma protobuf en `text/plain`.

`{role_id}` est un rôle détenu par l'appelant, ou un ensemble de rôles détenus séparés par des virgules (`analyst,org_admin`), servi comme le méta-rôle de l'ensemble : l'union de ce qui est accordé à ses membres. Un rôle que l'appelant ne détient pas est refusé avec `403 auth.role_not_assigned`, et un identifiant de méta-rôle nommé directement avec `403 auth.meta_role_named`. `?domains=a,b` restreint le proto à ces domaines ; un domaine que le rôle n'atteint pas est refusé avec `403 data.domain_not_accessible`. Un rôle sans surface de données répond `404 data.no_proto_for_role`, un rôle inconnu `404 data.no_role` et un modèle pas encore construit `503 data.schema_cache_not_ready`.

Chaque table du schéma du rôle produit un `message` proto. Les relations produisent des champs de message imbriqués. Le service ne liste des RPC que pour les tables à la racine du schéma du rôle, les mêmes que celles de son type `Query` GraphQL ; une table liée a son message et s'atteint par sa relation.

Ajoutez `?domains=a,b` pour restreindre le fichier à ces domaines. Le service liste alors des RPC pour les tables propres aux domaines choisis ; les tables qui leur sont liées apparaissent comme messages.

Le fichier suit le modèle. Il est régénéré avec le schéma du rôle chaque fois que le modèle ou le rôle change, y compris pour un méta-rôle. Correspondance des types : `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`.

---

### `GET /data/subscribe/{table}`

Flux Server-Sent Events pour les notifications de changement en temps réel d'une table.

La livraison des notifications utilise un fournisseur enfichable choisi selon le type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY` (via asyncpg), les sources MongoDB utilisent Change Streams (`collection.watch()`), et les sources Kafka utilisent des groupes de consommateurs. Chaque fournisseur implémente une interface de surveillance asynchrone commune. Le filtrage par sécurité au niveau des lignes et la validation de schéma s'appliquent quel que soit le fournisseur. Les sources WebSocket et RSS sont également prises en charge.

**En-tête — `X-Provisa-Sink` :** définissez-la sur une cible Kafka (p. ex. `kafka://broker:9092/topic`) pour rediriger les événements de changement vers un puits Kafka au lieu de la réponse SSE. Le serveur lance un consommateur de puits et renvoie `202 Accepted` plutôt qu'un flux ouvert.

### `POST /data/sources/{source_id}/signal`

Indique à Provisa qu'une source a changé, sans lignes : le récepteur du signal de changement `signal`. Chaque table de la source dont le signal de changement est `signal` (le sien, sinon celui de sa source) est traitée comme écrite, comme pour une écriture via Provisa : ses réponses en cache sont supprimées, les vues matérialisées qui la lisent deviennent périmées et une construction de sa réplique est demandée. La construction d'une table protégée en charge attend sa fenêtre hors pointe. `?table=schema.table` signale une table.

Le rôle de l'appelant a besoin du droit d'écriture et doit atteindre le domaine de chaque table qu'il signale. La réponse est `202 Accepted` avec les tables signalées :

```json
{"signalled": ["public.orders"]}
```

Une source qui n'existe pas, ou une `table` qu'elle n'a pas, répond `404` ; une source sans table sur le signal de changement `signal` répond `409`, car rien ne serait rafraîchi.

---

## Points de terminaison REST admin {: #admin-rest-endpoints }

### Config

#### `GET /admin/config`

Télécharge le `provisa.yaml` actuel en `application/x-yaml` avec un en-tête `Content-Disposition: attachment`.

#### `PUT /admin/config`

Téléverse un YAML de configuration révisé. Le serveur écrit une sauvegarde `.bak`, enregistre le nouveau fichier, recharge à partir de lui les réglages du déploiement et l'applique au magasin du modèle comme un amorçage explicite et unique : ce que déclare le fichier est ajouté ou mis à jour via le magasin du modèle, et rien n'est retiré. Un redémarrage ultérieur lit les réglages du fichier et n'applique plus jamais son modèle ; après le premier démarrage, seul le magasin du modèle possède le modèle.

**Corps de la requête :** contenu YAML brut.

**Réponse :**

```json
{"success": true, "message": "Config uploaded and applied"}
```

En cas d'échec du rechargement : `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Télécharge le modèle tel qu'il est, écrit sous forme de fichier de configuration : les réglages du fichier du déploiement avec chaque section du modèle lue depuis le magasin du modèle — magasins, régions, sources, domaines, rôles, produits de données, termes du glossaire, tables et leurs colonnes, relations, métriques, étiquettes et leurs attributions, filtres de lignes, commandes, webhooks, déclencheurs planifiés et règles de nommage. Appliqué à un magasin du modèle vide, le fichier construit le même modèle. Ce que le déploiement amorce dans chaque magasin (les sources intégrées et leurs tables, les domaines système, `org_admin` et `platform_admin`) est omis.

**Paramètre de requête :** `sections` (facultatif) — une liste de sections du modèle séparées par des virgules, par ex. `sources,tables`. Seules ces sections sont écrites ; un nom inconnu répond 400 `settings.export_unknown_section`.

L'endpoint est ouvert dans chaque déploiement à un appelant qui détient le droit sur les réglages du déploiement ; la section Fichier de configuration de la page d'administration le propose sous **Exporter le modèle**.

Renvoie `application/x-yaml` avec `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Renvoie les deux côtés du diff de configuration — `original` (la référence de démarrage) et `current` (l'état en direct) — normalisés de façon identique, afin que la comparaison ne montre que les changements réels, pas le réordonnancement ou la dérive des commentaires.

**Réponse :**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Génère un correctif diff unifié de la référence vers la configuration postée.

Envoyez le YAML révisé comme corps de la requête. La réponse est un fichier `text/x-patch` (`provisa.config.patch`) que `git apply` ou `patch` peut consommer directement — utile pour valider les changements de configuration pilotés par l'interface via un pipeline CI/CD.

---

### Paramètres {: #settings }

#### `GET /admin/settings`

Renvoie les paramètres de plateforme actuels en JSON.

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

Met à jour les paramètres de plateforme à l'exécution. Tous les champs sont optionnels — seules les clés présentes dans le corps sont mises à jour.

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
- `naming` : `domain_prefix`, `convention` — écrit dans le fichier de configuration et déclenche un rechargement de schéma
- `relationships` : `auto_track_fk` — gouverne uniquement le suivi des clés étrangères. Une relation reposant sur une table de jonction est déclarée à l'enregistrement de la table et n'est jamais inférée, donc ce paramètre ne l'atteint pas.
- `otel` : `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Réponse :**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Modèles IA {: #ai-models }

#### `GET /admin/ai-models`

Renvoie les affectations de modèles IA de l'organisation agissante, le registre des modèles vectoriels, et la limite de débit NL.

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

Les clés API ne sont jamais renvoyées — `api_keys_set` rapporte uniquement si chaque fournisseur a une clé configurée. Les changements prennent effet à la prochaine requête ; aucun redémarrage n'est nécessaire.

#### `PUT /admin/ai-models`

Met à jour les affectations de modèles IA de l'organisation, le registre des modèles vectoriels, ou la limite de débit NL. Prend effet à la prochaine requête.

#### `GET /admin/ai-models/vendors/{vendor}/models`

Renvoie les noms de modèles actuellement servis par un fournisseur, pour le sélecteur de modèle.

La liste est lue en direct depuis la propre API de liste de modèles du fournisseur en utilisant la clé configurée de l'organisation — ou l'identifiant de déploiement lorsqu'aucune clé d'organisation n'est définie. Un modèle publié après la sortie de cette version est sélectionnable le jour même où le fournisseur le sert.

Renvoie `400` lorsque le fournisseur ne publie aucune API de liste de modèles (saisissez le nom du modèle directement dans ce cas) ou lorsqu'aucune clé n'est disponible.

---

### Moteur de fédération {: #federation-engine }

#### `GET /admin/federation-engine`

Renvoie la sélection actuelle du moteur de fédération, sa configuration de connexion, et le registre complet des moteurs sélectionnables.

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

Persiste une sélection de moteur de fédération.

**Corps de la requête :**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

La sélection est écrite dans la configuration de plateforme. Elle prend effet après le prochain redémarrage du service — le moteur est choisi une seule fois au démarrage.

---

### Politique de domaine {: #domain-policy }

#### `POST /admin/domain-policy`

Change la politique de domaine de l'organisation agissante (`use_domains` / `default_domain`).

Le basculement ne retire rien. Il est refusé avec `409` et le code `settings.domain_policy_catalog_exists` tant que l'organisation agissante a un catalogue : une table enregistrée, une source ou un domaine que le déploiement n'a pas amorcé. Les `params` du refus comptent chaque type (`tables`, `sources`, `domains`). Supprimez d'abord ces objets, chacun par sa propre action, puis basculez. Lorsqu'un fichier de configuration déclare une partie de ce catalogue, le code est `settings.domain_policy_catalog_in_config` : le chargement suivant le restaurerait, la politique se définit donc dans le fichier de configuration.

**Corps de la requête :**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` efface le remplacement de l'organisation et revient au paramètre au niveau du déploiement. `use_domains: false` requiert `default_domain` (le domaine unique dans lequel toutes les tables atterrissent). La reconstruction du catalogue est synchrone ; la réponse revient une fois les schémas prêts.

---

### Observabilité {: #observability }

#### `GET /admin/traces/recent`

Renvoie jusqu'à N portées récentes complétées depuis le tampon de portées en mémoire.

**Paramètres de requête :** `limit` (par défaut 50, max 200)

**Réponse :** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Recharge à chaud un catalogue nommé dans le coordinateur du moteur de fédération via son API REST. Reconnecte la connexion interne de Provisa et réexécute le DDL OTel.

**Paramètres de requête :** `catalog` (par défaut `"otel"`)

**Réponse :**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Redémarre le conteneur du moteur de fédération (développement mono-nœud uniquement).

**Paramètres de requête :** `container` (par défaut la variable d'environnement `QUERY_ENGINE_CONTAINER`, puis `"trino"`)

---

### Découverte {: #discovery }

#### `POST /admin/discover/relationships`

Déclenche la découverte de relations. Exécute toujours l'introspection de clé étrangère depuis le moteur de fédération. Exécute l'inférence LLM si `ANTHROPIC_API_KEY` est défini.

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

Liste les candidats de relation en attente.

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Accepte un candidat et l'enregistre comme relation.

**Corps de la requête (optionnel) :** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rejette un candidat.

**Corps de la requête :** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Renvoie le nombre de candidats rejetés.

#### `DELETE /admin/discover/candidates/rejected`

Supprime tous les candidats rejetés.

---

### Exploration de source {: #source-crawl }

#### `POST /admin/sources/crawl`

Explore une source de données pour introspecter son schéma et enregistrer des tables.

---

### Recherche de tables de source {: #source-table-search }

#### `GET /admin/sources/{source_id}/tables/search`

Recherche des tables disponibles (pas encore enregistrées) dans une source par nom.

---

### Profilage de tables {: #table-profiling }

#### `POST /admin/tables/{table_id}/profile`

Exécute un profil de colonne sur une table enregistrée — cardinalité, min/max, taux de valeurs nulles.

---

### Descriptions de source {: #source-descriptions }

#### `POST /admin/source-meta/db-description`

Génère des descriptions assistées par LLM pour les tables et colonnes d'une source.

---

### Stockage objet {: #object-storage }

#### `GET /admin/org-storage`

Rapporte l'empreinte de stockage de l'organisation agissante par rapport à son allocation de plateforme, et si l'organisation a enregistré son propre magasin.

Lorsque l'organisation a enregistré son propre DSN, ses matérialisations y vont et ne sont plus comptées dans l'allocation. Le DSN lui-même n'est jamais renvoyé.

#### `PUT /admin/org-storage`

Enregistre (ou efface) le magasin de matérialisation propre à l'organisation.

**Corps de la requête :**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

Le DSN est validé contre le moteur de fédération avant d'être accepté — un DSN inutilisable échoue à l'enregistrement, pas des heures plus tard lors d'un rafraîchissement. La valeur est chiffrée au repos et jamais renvoyée par GET.

Envoyez `storage_url: null` pour effacer le magasin propre de l'organisation et renvoyer ses matérialisations au magasin de plateforme (et à l'allocation). L'exécution de l'organisation est reconstruite dans le même appel, de sorte que le nouveau magasin est effectif immédiatement.

---

### Chiffrement d'organisation {: #org-encryption }

#### `GET /admin/org-encryption`

Renvoie l'état de clé actuel de l'organisation : empreinte, id, et provenance. Ne renvoie jamais le matériel de clé.

Lorsque l'organisation n'a défini aucune clé, renvoie `{"configured": false}`. Chaque organisation démarre dans cet état et hérite de la clé du déploiement.

#### `PUT /admin/org-encryption`

Définit ou fait pivoter la clé de chiffrement au repos de l'organisation.

**Corps de la requête :**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omettez `key_b64` pour que Provisa génère une clé — la voie la plus sûre, car la clé n'apparaît jamais dans un presse-papiers ou un journal de requêtes. Fournir `key_b64` permet d'apporter votre propre clé.

La rotation ajoute une nouvelle entrée active à l'anneau de clés et conserve l'ancienne, de sorte que les données écrites sous la clé précédente restent lisibles. La rotation n'est pas un rechiffrement. Il n'existe aucun point de terminaison de suppression : retirer la dernière clé rendrait chaque charge utile enveloppée illisible.

L'anneau en direct est reconstitué dans le même appel, de sorte que la prochaine écriture chiffrée utilise immédiatement la nouvelle clé.

---

### Import Hasura / DDN {: #hasura-ddn-import }

#### `POST /admin/import/hasura/preview`

Convertit une archive de projet Hasura v2 ou DDN en configuration Provisa proposée sans rien écrire.

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

Charge une configuration précédemment prévisualisée dans l'organisation agissante.

**Corps de la requête :**

```json
{"config_yaml": "<yaml string>"}
```

Un import est un amorçage explicite et unique du magasin du modèle de l'organisation agissante, la même application que `PUT /admin/config` : il ajoute et met à jour via le magasin du modèle ce que déclare la configuration importée, avec ses gardes et ses versions, et ne retire rien de ce qu'elle ne mentionne pas. Il n'y a pas de champ `replace`. Les catalogues moteur des sources qu'elle déclare, les pools de l'organisation et ses schémas sont reconstruits avant le retour de la réponse.

**Réponse:** `{"summary": {...}}` — les décomptes de ce que déclare la configuration importée.

---

### Échange Apache Ossie {: #apache-ossie-interchange }

#### `GET /admin/ossie`

Exporte le modèle gouverné de l'organisation en tant que document YAML Apache Ossie (incubating).

Le document est dérivé de l'état en direct à chaque requête — jamais mis en cache — donc il ne peut pas être périmé. Les tables deviennent des objets `dataset`, les colonnes deviennent des objets `field`, et les relations sont mappées vers des objets `relationship` Ossie.

Renvoie `text/yaml` avec `Content-Disposition: attachment; filename=provisa.ossie.yaml`.

#### `GET /admin/dbt/sources`

Télécharge le catalogue gouverné sous forme de `sources.yml` dbt, tel qu'il est servi à un rôle.

**Paramètre de requête:** `role` — un rôle que détient l'appelant, ou n'importe quel rôle pour le détenteur de `access_config`.

Renvoie `text/yaml` avec `Content-Disposition: attachment; filename=provisa.sources.yml`. Un rôle inconnu est un `404` avec le code `dbt_sources.unknown_role`. Voir [intégrations](integrations.md#dbt-sources-file) pour le contenu du fichier.

#### `POST /admin/ossie/import`

Analyse un document Ossie YAML ou JSON et renvoie des propositions d'enregistrement de table et de relation.

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

### Actions (fonctions et webhooks) {: #actions-functions-and-webhooks }

Tous les points de terminaison sont sous le préfixe `/admin/actions`.

Chaque invocation — depuis GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, et Provisa gRPC — passe par un seul exécuteur gouverné qui applique l'affectation de la commande (`visible_to`), le droit `write` pour une mutation et la gouvernance de façon uniforme. Voir [docs/integrations.md](integrations.md#invoking-commands-across-protocols) pour la syntaxe d'appel par protocole.

#### `GET /admin/actions`

Renvoie toutes les fonctions DB et webhooks suivis.

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

Chaque objet webhook porte un booléen `approved`. Un webhook est approuvé une fois qu'un steward exécute sa demande de création ; les webhooks déclarés en configuration sont auto-approuvés. Un webhook non approuvé est enregistré mais n'est exposé sur aucune surface.

#### `POST /admin/actions/functions`

Enregistre une fonction suivie (commande).

**Champs clés :**

| Champ | Requis | Description |
| --- | --- | --- |
| `name` | Oui | Nom de commande unique |
| `kind` | Oui | `"query"` → champ GraphQL Query ; `"mutation"` → champ Mutation |
| `implKind` | Non | Comment la commande s'exécute — voir le tableau ci-dessous (par défaut `source_procedure`) |
| `binding` | Non | Détails de connexion spécifiques à `implKind` (objet JSON) |
| `returnSchema` | Non | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — rend la commande à ensemble de résultats sur chaque surface |
| `arguments` | Non | Définitions d'arguments `[{name, type}]` ; l'ordre positionnel compte pour les appelants SQL et Bolt |
| `visibleTo` | Non | ID de rôle auxquels la commande est affectée (vide : tous les rôles). Une `mutation` exige en plus le droit `write` |
| `domainId` | Non | Domaine pour le placement GraphQL et le contrôle d'accès |

**Valeurs `implKind` :**

| `implKind` | Ce qui s'exécute | Champs `binding` |
| --- | --- | --- |
| `source_procedure` | Procédure stockée sur une source enregistrée (par défaut) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script côté serveur | `script` |
| `http` | Appel HTTP sortant | `url`, `method` |
| `grpc` | Appel gRPC sortant vers un serveur externe | `target`, `method` |
| `python` | Callable Python hébergé par Provisa | `callable` (p. ex. `"demo.py_functions:random_dataset"`) |

Les commandes de démonstration `random_python_set` (`implKind: python`) et `random_grpc_set` (`implKind: grpc`) illustrent en pratique les commandes à ensemble de résultats avec `returnSchema` ; les deux figurent dans `config/provisa-install.yaml`.

#### `PUT /admin/actions/functions/{name}`

Met à jour une fonction suivie par nom.

#### `DELETE /admin/actions/functions/{name}`

Supprime une fonction suivie par nom.

#### `POST /admin/actions/webhooks`

Enregistre un webhook suivi. Enregistrer ou mettre à jour un webhook met en file une demande d'approbation de steward — le webhook devient actif sur toutes les surfaces uniquement après qu'un steward l'approuve. Les webhooks déclarés en configuration sont auto-approuvés. **Champs du corps de la requête :** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`.

#### `PUT /admin/actions/webhooks/{name}`

Met à jour un webhook suivi par nom. Toute modification réinitialise l'approbation en attente jusqu'à réapprobation.

#### `DELETE /admin/actions/webhooks/{name}`

Supprime un webhook suivi par nom.

#### `POST /admin/actions/test`

Teste une action (fonction ou webhook) par nom.

---

### Rôles

Tous les points de terminaison sont sous le préfixe `/admin/roles`.

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Liste tous les rôles |
| `POST` | `/admin/roles/` | Crée un rôle |
| `PUT` | `/admin/roles/{role_id}` | Met à jour un rôle |
| `DELETE` | `/admin/roles/{role_id}` | Supprime un rôle |

---

### Utilisateurs {: #users }

Tous les points de terminaison sont sous le préfixe `/admin/users`.

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

### Organisations {: #organizations }

Tous les points de terminaison sont sous `/admin/orgs`.

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

### Invitations {: #invites }

Tous les points de terminaison sont sous `/admin/invites`.

| Méthode | Chemin | Description |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Crée une invitation |
| `GET` | `/admin/invites/` | Liste les invitations en attente |
| `DELETE` | `/admin/invites/{token}` | Révoque une invitation |

---

### GraphQL admin {: #admin-graphql }

#### `POST /admin/graphql`

Point de terminaison GraphQL Strawberry pour toutes les opérations admin : CRUD de source et de table, gestion des relations, configuration de domaine, règles RLS, contrôle du cache, conventions de nommage, gestion des tâches planifiées, et compilation de requêtes.

Pour la référence complète du schéma — chaque champ Query, champ Mutation, et type d'entrée/sortie — voir la [Référence de l'API GraphQL admin](admin-graphql.md).

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

---

### Configuration initiale {: #setup }

#### `GET /setup/status`

Renvoie l'état de configuration au premier lancement. Toujours non authentifié.

#### `POST /setup/`

Termine la configuration au premier lancement.

---

## Vérification de santé {: #health-check }

#### `GET /health` ou `HEAD /health` {: #get-health-or-head-health }

Renvoie `{"status": "ok"}`. Toujours non authentifié.

---

## Réponses d'erreur {: #error-responses }

| Statut | Signification |
| --- | --- |
| 400 | Requête invalide, erreur de validation, ou erreur d'analyse SQL |
| 401 | Jeton d'authentification manquant ou invalide |
| 403 | Capacités insuffisantes ; violation de gouvernance |
| 404 | Rôle, ressource, ou fichier de configuration introuvable |
| 422 | En-tête requis manquant (p. ex. `X-Provisa-Role`) |
| 503 | Base de données ou source non connectée ; dépendance indisponible |
| 504 | Délai de requête dépassé |

Les violations de gouvernance sur `POST /data/sql` renvoient un statut HTTP 403 avec un corps structuré :

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

## Point de terminaison Arrow Flight {: #arrow-flight-endpoint }

Port `8815`. Transport columnaire Arrow natif sur gRPC.

Les requêtes et la découverte de catalogue sont toutes deux disponibles sur la même connexion. Le pipeline de gouvernance complet (sécurité au niveau des lignes, masquage, échantillonnage) est appliqué à chaque requête.

La découverte du catalogue (`list_flights`, `get_flight_info`, `get_schema` et un ticket sans `query`) liste ce qui est accordé au rôle de l'appelant. Avec l'authentification activée, les trois appels sans ticket prennent l'identifiant dans l'en-tête d'appel `authorization: Bearer` et le rôle dans `x-provisa-role`, et sont refusés sans identifiant ; voir [Intégrations](integrations.md).

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

Lorsque le proxy Zaychik Flight SQL est disponible (port 8480), les lots d'enregistrements circulent de bout en bout sans matérialisation complète. Repli sur la matérialisation via la couche de requête fédérée si Zaychik n'est pas disponible.

**Avertissements.** Ce qu'une réponse dit d'elle-même voyage dans le flux comme `app_metadata` d'un lot sans ligne : `{"provisa_warnings": [{"code", "params", "message"}]}`. Les avertissements connus avant la lecture des lignes arrivent dans un lot sans ligne qui les précède. Un avertissement connu seulement après la lecture des lignes arrive dans un second lot sans ligne, le dernier du flux : `statement.rows_cut` indique que la réponse a été coupée à une limite de lignes et que d'autres lignes correspondent. Lisez `app_metadata` sur chaque lot, le dernier compris.

---

## Point de terminaison gRPC Protobuf {: #protobuf-grpc-endpoint }

Port `50051` (remplacer avec la variable d'environnement `GRPC_PORT` ou la configuration `server.grpc_port`).

Passez le rôle dans la clé de métadonnées gRPC `x-provisa-role`. Si absente, le serveur abandonne avec `UNAUTHENTICATED`.

Téléchargez le proto spécifique au rôle depuis `GET /data/proto/{role_id}`. Seules les tables et colonnes visibles pour ce rôle apparaissent.

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Chaque table à la racine du schéma du rôle produit une RPC en streaming `Query{TypeName}`. Ce sont les tables que liste le type `Query` GraphQL du rôle. Une table que le rôle n'atteint que par une relation n'a pas de RPC propre, de même qu'elle n'a pas de champ racine dans GraphQL. Les RPC `Insert{TypeName}` existent par symétrie de schéma mais échouent avec `UNIMPLEMENTED`.

**Paramètres.** Une table à paramètres (une table enregistrée sur une API, ou une table de requête) a un message `{TypeName}Params`, un champ optionnel par paramètre, nommé sans son préfixe `_nf_`, et `Query{TypeName}Request` le porte en `params`. Chaque valeur qui y est fixée atteint la source comme le paramètre, liée dans l'instruction ; un nom qui n'est pas un paramètre de la table est refusé avec `INVALID_ARGUMENT`, et une lecture sans un paramètre obligatoire est refusée en le nommant. Le proxy HTTP gRPC prend les mêmes valeurs dans l'objet `params` du corps (HTTP 400, code `data.invalid_parameter`, pour un nom inconnu ou une valeur du mauvais type JSON).

Une méthode qui ne fait pas partie du service du rôle de l'appelant répond `UNIMPLEMENTED`.

`grpc_reflection.v1alpha` est activé pour la découverte de services sans proto précompilé. La réflexion décrit à chaque appelant le service du rôle sous lequel il agit : le même `.proto` que ce rôle télécharge depuis `GET /data/proto/{role_id}`. Elle liste par leur nom le service Provisa et le service de réflexion, et décrit le fichier du service Provisa.

Un appel de réflexion est identifié comme tout autre appel gRPC. Avec l'authentification activée, le rôle provient des identifiants de l'appelant. Avec l'authentification désactivée, indiquez le rôle dans `x-provisa-role` ; sur un déploiement qui sert plus d'une organisation, indiquez aussi l'organisation dans `x-provisa-org`. Un appel sans ces métadonnées est refusé avec `UNAUTHENTICATED`. Avec l'authentification désactivée, la réflexion reste désactivée tant que `server.grpc_allow_unsecured_reflection` n'est pas défini.

```bash
grpcurl -plaintext -H 'x-provisa-role: analyst' localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 provisa.v1.ProvisaService/QueryOrders
```

Le serveur gRPC démarre lorsque le modèle chargé au démarrage définit un service gRPC. Chaque appel est ensuite servi à partir du modèle tel qu'il est : une table enregistrée ou modifiée pendant que le serveur tourne est servie et décrite dès l'appel suivant, sans redémarrage.

#### RPC d'agrégation et de regroupement {: #aggregate-and-group-by-rpcs }

Lorsqu'une table a `enable_aggregates` défini, le proto généré inclut deux RPC supplémentaires aux côtés de `Query{TypeName}` :

- **`Query{TypeName}Aggregate`** — renvoie des scalaires d'agrégation pour la table (`count` ; `sum`, `avg`, `stddev`, `variance` par colonne numérique ; `min`, `max` par colonne comparable)
- **`Query{TypeName}GroupBy`** — renvoie une ligne par clé de groupe avec des sous-champs d'agrégation et, optionnellement, des scalaires de table de base et des lignes de dimension jointe dans un champ `nodes`

Les deux passent par le même pipeline d'agrégation du compilateur que les champs racines `{field}_aggregate` et `{field}_group_by` de GraphQL — aucune implémentation d'agrégation distincte.

**Champ `funcs`.** Le message de requête accepte un champ de chaîne répétée `funcs`. Les valeurs valides sont `count`, `sum`, `avg`, `stddev`, `variance`, `min`, et `max`. Lorsque `funcs` est omis, chaque fonction que le schéma expose pour cette table est demandée. Lorsqu'il est défini, seules les fonctions nommées apparaissent. Si aucune des fonctions nommées ne s'applique aux types de colonnes de la table, la requête se replie sur `count`.

**Champs `include_nodes` et `include`.** Les requêtes `Query{TypeName}GroupBy` peuvent définir `include_nodes: true` pour inclure les colonnes scalaires de la table de base dans le champ `nodes` de chaque ligne. Le champ de chaîne répétée `include` nomme les champs de relation plusieurs-à-un dont les colonnes scalaires sont aussi imbriquées dans `nodes`. Cela correspond au comportement JSON:API `?includeNodes=` / `?include=`.

---

## Pilote JDBC {: #jdbc-driver }

Le pilote JDBC Provisa (`provisa-jdbc-<release>.jar`) expose le catalogue sémantique aux outils BI (Tableau, PowerBI, DBeaver).

**URL de connexion :** `jdbc:provisa://host:port`

Les domaines sont mappés en schémas JDBC. Les tables utilisent leurs alias enregistrés. Les colonnes utilisent des alias et font apparaître les descriptions comme `REMARKS`. Les méthodes de métadonnées standards (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exposent les relations sémantiques comme métadonnées PK/FK.

**Prise en charge SQL :** `SELECT * FROM <alias> [WHERE col = 'value']`.

Le pilote demande une redirection Arrow IPC par défaut. Les résultats circulent lot par lot via `ArrowStreamReader`, limités à un lot d'enregistrements en mémoire.

---

## Format de l'argument `orderBy` {: #orderby-argument-format }

L'argument `order_by` utilise des objets `{column: direction}` avec une énumération de direction à 6 valeurs :

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Directions prises en charge : `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`.

---

## Abonnements {: #subscriptions }

Les abonnements SSE sont disponibles à `GET /data/subscribe/{table}`. La livraison des notifications utilise un fournisseur enfichable sélectionné selon le type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY`, les sources MongoDB utilisent Change Streams, et les sources Kafka utilisent des groupes de consommateurs. Le filtrage par sécurité au niveau des lignes et la validation de schéma s'appliquent quel que soit le fournisseur. Les sources WebSocket et RSS sont également prises en charge via le même point de terminaison.

---

## Glossaire métier {: #business-glossary }

Le glossaire métier fait correspondre les noms de champs physiques — tels qu'ils existent dans les bases de données sources — à un vocabulaire humain partagé. Chaque colonne enregistrée dans le modèle reçoit automatiquement un terme. Aucune saisie manuelle n'est requise pour peupler le glossaire ; les curateurs ajoutent des définitions, des relations et des experts par-dessus ce que le système dérive.

### Comment les termes sont dérivés {: #how-terms-are-derived }

Lorsque Provisa enregistre ou met à jour les colonnes d'une table, `normalize_term` (`provisa/core/glossary.py`) s'exécute sur chaque nom de colonne et produit une expression canonique.

La normalisation applique cinq règles en séquence :

1. Découper aux frontières camelCase et aux caractères séparateurs (`_`, `-`, `.`, `/`, espace).
2. Mettre le résultat en minuscules.
3. Développer une table d'abréviations fixe (p. ex. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Retirer un **jeton de substitution** final (`identifier`, `code`, `index`, ou `reference`) — une colonne nommée d'après sa clé ou son code pointe vers le concept sous-jacent via une valeur de substitution, donc le terme devrait être le concept lui-même. Le dernier jeton restant n'est jamais retiré.
5. Qualifier une **expression trop générique** avec le concept de la table. Lorsque l'expression normalisée complète est un simple mot d'attribut (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, et similaires), le terme devient `<table concept> <phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Un seul terme `name` partagé entre des tables non liées fusionnerait des significations distinctes ; la qualification relie chaque colonne à son concept englobant à la place. Le concept de table est le nom métier de la table, normalisé avec un nom de tête singulier (`order_lines` → `order line`).

Les pseudo-colonnes de filtre natif (préfixées `_nf_`, ou toute colonne portant `native_filter_type`) sont de la mécanique de paramètres de requête, pas des champs métier, et ne dérivent aucun terme.

Parce que `id`, `key`, `pk`, et `sk` se développent tous en `identifier` avant la vérification du jeton de substitution, trois noms de colonnes physiquement différents aboutissent exactement au même terme :

| Nom physique | Après normalisation |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Les trois premiers se réduisent à un seul terme. `transaction amount` conserve les deux jetons parce que `amount` n'est pas un jeton de substitution. Une colonne `id` isolée — sans jeton précédent — ne peut pas être retirée ; elle se normalise en `identifier` afin que le terme ne soit pas vide.

### Cycle de vie {: #lifecycle }

Les termes sont **dérivés de l'appartenance au modèle**, pas créés à la demande par les utilisateurs. Le référentiel de table est le chemin d'écriture unique : `sync_table_refs` s'exécute à l'intérieur de chaque mise à jour d'ensemble de colonnes, et `sweep_refless_terms` s'exécute après tout chemin de suppression.

**Lorsqu'une colonne est ajoutée :** Provisa recherche le terme normalisé par nom. S'il existe déjà, la colonne obtient une référence vers lui (et si le terme était déprécié, il est réactivé — `deprecated` est remis à `False`). Si aucun terme n'existe encore, un est créé.

**Lorsqu'une colonne disparaît** (changement de schéma ou suppression de table) : sa référence est supprimée et le terme est **réglé** selon une règle de suppression-ou-dépréciation. Un terme ancré sans référence restante est supprimé purement et simplement — avec ses arêtes et affectations d'experts — sauf si le supprimer laisserait un terme abstrait déconnecté de tous les termes ancrés (aucun chemin dans le graphe de termes). Dans ce cas, le terme est **déprécié** (marqué `deprecated=True`) plutôt que supprimé, afin que l'ancrage du graphe du terme abstrait survive.

Les termes abstraits ne sont jamais supprimés automatiquement ; ils existent en dehors du cycle de vie physique et ne sont supprimés qu'explicitement via l'API admin.

**Réactivation :** si le nom normalisé d'un terme déprécié réapparaît (une colonne est réenregistrée), le terme est démarqué et ses références recommencent à s'accumuler.

### Points de terminaison de curation {: #curation-endpoints }

Tous les points de terminaison sont sous `/admin/glossary`. Ils requièrent un accès `org_admin` et une organisation configurée. Chaque mutation déclenche une publication de métadonnées.

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

Valeurs `rel_type` valides : `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`.

**Corps de `POST /admin/glossary/terms/{term_id}/experts` :**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valeurs `kind` valides : `expert`, `author`.

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

Définir `export_excluded` à `true` retient le terme de tous les instantanés d'export de métadonnées, indépendamment de ses références physiques ou de son statut abstrait. Le remettre à `false` restaure le terme dans l'instantané à la prochaine publication. Les données de curation (définition, arêtes, experts) ne sont pas affectées.

### Curation assistée par IA {: #ai-assisted-curation }

Le modèle IA configuré de l'organisation peut rédiger des définitions et proposer des arêtes de relation à travers tout le glossaire en une seule opération. Les deux actions groupées requièrent un accès `org_admin` et une organisation configurée.

**`POST /admin/glossary/definitions/generate`**

Parcourt chaque terme du glossaire, ignore ceux qui ont déjà une définition, et appelle le modèle IA de l'organisation pour en rédiger une pour chaque terme restant. Le brouillon est persisté immédiatement — contrairement au point de terminaison de brouillon par terme (`POST /admin/glossary/terms/{term_id}/definition/generate`), il n'y a pas d'étape d'édition. Les définitions rédigées par des humains ne sont jamais écrasées : la garde est `if summary["definition"]: continue` avant tout appel au modèle. Une seule notification de publication couvre tout le lot.

Réponse :

```json
{"generated": 12}
```

`generated` est le nombre de termes ayant reçu une nouvelle définition. Il est de zéro lorsque chaque terme en a déjà une.

**`POST /admin/glossary/relationships/generate`**

Envoie la liste complète des termes au modèle IA de l'organisation avec une invite qui spécifie les dix types d'arêtes autorisés (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) et demande uniquement des propositions sûres. Le modèle répond avec un tableau JSON ; chaque entrée est validée avant toute écriture : les noms de termes inconnus, les auto-arêtes, et les types d'arête en dehors de l'énumération fermée sont silencieusement rejetés. Les propositions valides sont insérées de façon idempotente — réexécuter l'action ne duplique pas les arêtes. Une seule notification de publication couvre le lot. Le point de terminaison renvoie `{"added": 0}` immédiatement lorsque le glossaire contient moins de deux termes non dépréciés.

Réponse :

```json
{"added": 5}
```

`added` est le nombre d'arêtes écrites. Une arête déjà existante compte quand même — l'insertion réussit, mais les données de l'arête ne changent pas.

### Outil MCP `search_terms` {: #mcp-search_terms-tool }

```
search_terms(query, role=None, limit=25)
```

Recherche les noms et définitions de termes avec une correspondance de sous-chaîne insensible à la casse, jusqu'à `limit` résultats. Chaque résultat est le détail complet du terme : `name`, `definition`, `is_abstract`, `deprecated`, références physiques (avec `source_id`, `schema_name`, `table_name`, `column_name`), arêtes typées, et affectations d'experts.

Utilisez `search_terms` avant d'écrire du SQL pour trouver chaque champ physique qui représente un concept par nom. Par exemple, rechercher `"order date"` renvoie le terme et toutes les colonnes `order_dt`, `orderDate`, `ORDER_DATE` à travers chaque table enregistrée.

### Outils statistiques MCP {: #mcp-statistics-tools }

```
describe(sql, role=None)
correlate(sql, columns, method="pearson", role=None)
regress(sql, target, features, kind="linear", role=None)
compare(sql, value, group, role=None)
forecast(sql, time, value, horizon, seasonal_periods=None, role=None)
```

Chaque outil prend un jeu de données défini par `sql` et l'exécute par le même pipeline gouverné que `run_sql`, sous le rôle de l'appelant : les règles de lignes, le masquage et l'expurgation s'appliquent, un SQL que le pipeline refuse échoue comme pour `run_sql`, et un rôle que l'appelant ne détient pas est refusé. Chacun renvoie des résultats, jamais les lignes, avec la méthode utilisée et le `n` compté.

| Outil | Renvoie |
| --- | --- |
| `describe` | Par colonne : `count`, `missing`, `distinct` ; pour un nombre `mean`, `sd`, `min`, `max` et `quantiles` (p0, p5, p25, p50, p75, p95, p100) ; `top_values` (les cinq plus fréquentes) |
| `correlate` | La `matrix` de corrélation `pearson` ou `spearman` des `columns` numériques, sur les lignes complètes dans toutes, et ce `n` |
| `regress` | `kind="linear"` : `coefficients` des moindres carrés (`estimate`, `se`, `p_value`), `r_squared`, `adj_r_squared`. `kind="logistic"` : une `target` à deux valeurs, `coefficients` du maximum de vraisemblance, `pseudo_r_squared`, `auc` et la `positive_value`. Une variable texte devient un indicateur par valeur sauf la première |
| `compare` | Le `n`, la `mean` et le `sd` de chaque groupe ; pour deux groupes, le test t de Welch (`t`, `df`, `p_value`) et le d de Cohen ; au-delà, l'ANOVA à un facteur (`f`, `df_between`, `df_within`, `p_value`) et l'êta carré |
| `forecast` | `horizon` points après le dernier `time`, chacun avec `value`, `lower` et `upper` (intervalle de prévision à 95 %), par lissage exponentiel (ETS) à erreur additive, avec une tendance additive à partir de 10 points et une saisonnalité additive quand `seasonal_periods` est donné |

Le moteur calcule ce qu'il peut sur chaque ligne que lit le rôle : les comptages, comptages distincts, moyennes, écarts-types, minimums et maximums de `describe`, et le `n`, la moyenne et la variance de chaque groupe pour `compare`. Un modèle ajusté ainsi que les quantiles et valeurs les plus fréquentes de `describe` lisent au plus `mcp.stats_max_rows` lignes (Admin → Settings → Limits ; 100 000 par défaut). Chaque réponse qui lit des lignes donne `rows_read`, `row_cap` et `truncated`, et la limite de lignes propre à un rôle est signalée sous `warnings`, comme pour `run_sql`.

Les outils statistiques ne renvoient que des résultats, donc les lignes derrière une analyse n'entrent jamais dans le contexte du modèle : minimisation des données par défaut. Le contrôle d'accès est ce qui limite ce qu'un agent peut apprendre. Pour n'exposer que des agrégats, publiez-les sous forme de vues matérialisées et accordez aux rôles ces vues, pas les tables sous-jacentes ; des agrégats sur de petits groupes peuvent identifier des individus, et la vue matérialisée fixe le regroupement que lit un rôle. Voir [Sécurité](security.md#statistics-over-mcp).

### Export de métadonnées {: #metadata-export }

Le graphe de termes du glossaire est inclus dans chaque `MetadataSnapshot` construit par `build_snapshot`.

L'export applique les mêmes filtres que le reste de l'instantané :

- Un terme marqué `export_excluded` est retenu purement — indépendamment de ses références physiques, de son statut abstrait, ou du fait que le catalogue de l'organisation soit configuré.
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

La propriété est le lien contraignant, pas le nom : l'id fournisseur de chaque terme publié est capturé dans `catalog_bindings` sous l'URN du terme (`provisa://<org>/terms/<name>`), et Provisa ne modifie ou ne supprime un élément de glossaire côté fournisseur que lorsqu'il détient ce lien (ou que l'élément vit dans le conteneur propriété de Provisa qu'il a créé). Un élément de glossaire sans lien Provisa provient du système externe et n'est jamais touché ; les mises à jour lisent-fusionnent afin que les champs ajoutés par le steward sur les propres termes de Provisa survivent ; rien n'est supprimé lorsqu'un terme quitte l'instantané. Les affectations terme-vers-actif du steward restent la propriété de l'externe — aucun adaptateur n'écrit d'affectations terme-vers-actif (la publication des affectations rédigées par Provisa est un suivi explicite). Sur Collibra spécifiquement, la sécurité sous la sémantique REPLACE de l'API Import repose sur le confinement : la charge utile ne mentionne que des actifs à l'intérieur du domaine de glossaire Provisa et des instances de relation uniquement entre termes Provisa, de sorte que les glossaires du steward et leurs relations ne sont jamais atteignables.

---

## Produits de données {: #data-products }

Un produit de données regroupe des tables publiées ensemble pour la consommation, possédées par exactement un domaine. Les champs suivent le vocabulaire ODPS (Open Data Product Standard) là où Provisa possède déjà la source de vérité. L'interface admin expose les produits de données sous **Admin → Data Products**.

### Capacités {: #capabilities }

| Capacité | Accorde |
| --- | --- |
| `data_product_read` | Accès en lecture au champ de requête `data_products` et à la page admin Produits de données. Semé par défaut à `org_admin`, `analyst`, `developer`, et `modeler`. |
| `data_product_rw` | Mutations de création et de suppression. Active les contrôles Nouveau / Modifier / Supprimer dans l'interface. |

### GraphQL admin {: #admin-graphql_1 }

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

Supprimer un produit efface `product_id` de chaque table membre, supprimant leur appartenance. Requiert `data_product_rw`.

### Schéma de champs {: #field-schema }

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

Deux champs supplémentaires existent sur le modèle mais ne sont pas exposés dans le `DataProductType` / `DataProductInput` Strawberry — ils sont spécifiques à Snowflake Horizon Catalog :

| Champ | Notes |
| --- | --- |
| `support_contact` | E-mail ou URL ; requis par les manifestes de fiche d'organisation Horizon Catalog |
| `publish` | `true` pour publier immédiatement les fiches Horizon ; les nouvelles fiches sont en DRAFT par défaut |

### Appartenance de table {: #table-membership }

Une table rejoint un produit de données en définissant son champ `product_id` au formulaire d'édition de table. Le sélecteur est délimité aux produits dont le `domain_id` correspond au domaine propre de la table — une table du domaine `marketing` n'est jamais proposée un produit du domaine `sales`.

Les commandes du même domaine peuvent aussi être affectées comme membres.

### Filtre d'export de métadonnées {: #metadata-export-filter }

`build_snapshot` applique `data_products_only=True` pour chaque publication de catalogue. Les tables sans `product_id` sont retenues de l'instantané, ainsi que leurs arêtes de relation, arêtes de traçabilité, et étiquettes de gouvernance. Les sources et domaines sont toujours publiés. Les termes du glossaire ne publient que lorsqu'au moins une de leurs références physiques appartient à une table exportée (membre d'un produit).

Un produit sans membre exporté ne construit pas d'entrée d'instantané — une fiche sans membre représenterait mal le produit auprès du catalogue.

### Prise en charge des produits de données par cible de catalogue {: #data-product-support-by-catalog-target }

`MetadataSnapshot.data_products` atteint chaque adaptateur, mais seuls les adaptateurs dont la plateforme a un concept natif de produit de données le publient comme entité de premier ordre ; les autres publient les tables membres (déjà filtrées ci-dessus) sans regroupement en produit.

| Cible | Représentation du produit de données |
| --- | --- |
| Snowflake Horizon | Chaque produit devient un `SHARE` sur les adresses physiques de ses tables membres, enveloppé dans un `CREATE ORGANIZATION LISTING` interne — un produit de données natif Horizon Catalog. `publish=true` publie la fiche en direct immédiatement ; sinon elle atterrit en DRAFT. |
| BigQuery Dataplex | Chaque produit devient une fiche Analytics Hub via `/v1/dataProducts`. |
| OpenMetadata | Chaque produit devient une entité `DataProduct` native (`/api/v1/dataProducts`), avec une propriété dérivée du domaine. |
| DataHub | Chaque produit devient une entité `dataProduct` native (`urn:li:dataProduct:...`) avec ses propres aspects `dataProductProperties`/propriété. |
| Collibra | Chaque produit devient un actif d'un type de communauté `Data Product`, lié à ses tables membres via une relation `Data Product groups Table`. |
| Atlan | Publié comme une estimation de typedef personnalisé `DataProduct` — Atlan n'a pas de nom de type stable documenté pour ce concept, donc le mappage est au mieux. |
| Apache Atlas | Publié comme un typedef personnalisé `provisa_data_product` avec une relation `provisa_data_product_members` — Atlas n'a pas de type d'entité de produit de données natif. |
| OpenLineage | Pas une entité de premier ordre — les tables membres portent une facette personnalisée `provisa_data_product` nommant le produit propriétaire. |
