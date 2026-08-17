# Référence API

## Vue d'ensemble

Provisa expose des endpoints REST sous deux préfixes : `/data` pour l'exécution de requêtes et l'introspection de schéma, et `/admin` pour la gestion de configuration. (REQ-043) La plupart des endpoints de données nécessitent un identifiant de rôle. Les opérations de configuration admin utilisent une API GraphQL Strawberry sur `/admin/graphql`. (REQ-164)

---

## Authentification

Quand `auth.provider` est configuré dans `provisa.yaml`, tous les endpoints sauf `/health` et `/setup/status` nécessitent un en-tête `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Sans auth configurée, le serveur tourne en mode dev. Toute requête est traitée comme l'identité `anonymous`, qui se mappe vers tous les rôles configurés avec un accès domaine wildcard. (REQ-535)

**Connexion (`POST /auth/login`)** est fournie par le fournisseur d'auth actif quand `provider: basic` est configuré. (REQ-124) Le format des identifiants et la réponse dépendent du fournisseur.

**Introspection d'identité :**

```http
GET /auth/me
```

Retourne l'id, l'email, le nom d'affichage, les appartenances d'org, et les affectations de rôle de l'utilisateur authentifié. En mode dev, retourne `dev_mode: true` avec tous les ID de rôle listés. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Retourne `{"provider": "<name>"}` ou `{"provider": null}` quand l'auth n'est pas configurée. [tool-verified: `provisa/api/auth_router.py`]

---

## Endpoints de données

### `POST /data/graphql`

Exécute une requête ou mutation GraphQL. (REQ-043) [tool-verified: `provisa/api/data/endpoint.py:151`]

**Corps de requête :**

```json
{
  "query": "{ orders(where: {region: {eq: \"us\"}}) { id amount } }",
  "variables": {},
  "role": "admin",
  "extensions": {}
}
```

Le champ `role` n'est utilisé qu'en mode dev (sans auth). Quand l'auth est active, le rôle de l'utilisateur authentifié est utilisé et `role` dans le corps est ignoré.

Le champ `extensions` prend en charge le protocole Automatic Persisted Query (APQ) : (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**En-têtes :**

- `X-Provisa-Role` — surcharge le rôle (mode dev)
- `Accept` — format de réponse (voir Négociation de contenu)
- `Authorization` — `Bearer <token>` quand l'auth est activée
- `X-Provisa-Redirect-Format` — type MIME pour la sortie de redirection S3 (REQ-137)
- `X-Provisa-Redirect-Threshold` — nombre de lignes au-delà duquel la redirection se déclenche (REQ-137)
- `X-Provisa-Redirect` — `true` pour forcer la redirection inconditionnellement (REQ-029)

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

**Réponse (multi-racine avec mix en ligne/redirection) :**

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

Les requêtes multi-racine exécutent chaque champ racine indépendamment. Les champs sous le seuil de redirection retournent en ligne ; les champs au-dessus redirigent. La clé `redirects` (pluriel) mappe les noms de champ vers l'info de redirection. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**En-têtes de cache :**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (sur HIT) (REQ-536)

**Capacités requises :** `QUERY_DEVELOPMENT` pour toutes les requêtes, y compris l'introspection. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Négociation de contenu

| En-tête Accept | Format |
| --- | --- |
| `application/json` | JSON (défaut) |
| `application/x-ndjson` | JSON délimité par des retours à la ligne |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirection

Les résultats au-dessus d'un seuil de lignes configuré (ou quand `X-Provisa-Redirect: true`) sont écrits sur S3 et une URL présignée est retournée. (REQ-029, REQ-044)

| Format de redirection | Écrit par | Mémoire |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS fédéré | Aucune — les données ne passent jamais par Provisa |
| `application/x-orc` | CTAS fédéré | Aucune — les données ne passent jamais par Provisa |
| `application/json` | Provisa | Lié à la mémoire |
| `application/x-ndjson` | Provisa | Lié à la mémoire |
| `text/csv` | Provisa | Lié à la mémoire |
| `application/vnd.apache.arrow.stream` | Provisa | Lié à la mémoire |

Pour les exports analytiques volumineux, utilisez la redirection Parquet ou ORC. Le moteur de fédération écrit directement vers S3 en parallèle — aucune donnée ne passe par Provisa. (REQ-138)

```yaml
X-Provisa-Redirect-Format: application/vnd.apache.parquet
X-Provisa-Redirect-Threshold: 1000
```

---

### `POST /data/sql`

Exécute du SQL brut à travers le pipeline de gouvernance Stage 2. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:62`]

**Corps de requête :**

```json
{
  "sql": "SELECT id, amount FROM orders WHERE region = 'us'",
  "role": "admin"
}
```

**Capacités requises :** `QUERY_DEVELOPMENT`.

Les violations de gouvernance sur `POST /data/sql` retournent HTTP 403. (REQ-002, REQ-266)

**Réponse :** Même format que `/data/graphql` (lignes JSON par défaut, négocié via `Accept`).

---

### `POST /data/query`

Endpoint de requête unifié. Accepte GraphQL, SQL, ou Cypher — la syntaxe est auto-détectée. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Les requêtes Cypher peuvent aussi être soumises à l'endpoint Cypher uniquement `POST /query/cypher`. (REQ-345)

**Corps de requête :**

```json
{
  "query": "{ orders { id } }",
  "params": {},
  "variables": {},
  "role": "admin"
}
```

Retourne `{"data": ...}` pour GraphQL, `{"columns": [...], "rows": [...]}` pour SQL et Cypher.

---

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST simple auto-généré pour chaque table enregistrée. La chaîne de requête se mappe vers des arguments GraphQL et la requête se compile et s'exécute à travers le même pipeline (RLS, masquage, routage) que GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Paramètres de requête :**

- `limit` — nombre max de lignes (≥ 1)
- `offset` — sauter des lignes (≥ 0)
- `fields` — noms de colonne séparés par des virgules (défaut : tous les champs scalaires)
- `filter` — tableau JSON d'objets de filtre `{"field", "comparator", "value"}`
- `orderBy` — tableau JSON d'objets de tri `{"field", "direction"}`

Le rôle authentifié est requis ; les requêtes non authentifiées retournent `401`. Une spec OpenAPI pour ces routes est servie à `GET /data/rest/openapi.json` avec Swagger UI à `GET /data/rest/docs`.

#### Explorateur OpenAPI / Swagger UI

La page explorateur OpenAPI (`/app/openapi`) embarque Swagger UI dans un iframe sandboxé. La spec est scopée par rôle — seules les tables et colonnes visibles pour le rôle actuel apparaissent — et optionnellement filtrée par domaine via le sélecteur de domaine. L'UI bascule automatiquement entre thèmes clair et sombre. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

La page charge le HTML de la spec via `fetch()` plutôt qu'un `src` d'iframe direct, de sorte que la requête porte le jeton bearer de la session et que les propres requêtes relatives de Swagger UI se résolvent correctement contre la même origine. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

Quand elle est atteinte depuis un lien NL « Ouvrir dans OpenAPI », la page auto-étend l'endpoint cible, remplit les paramètres de requête depuis l'URL générée par NL (ex. `aggregate`, `groupBy`), et clique sur Execute — en utilisant du polling DOM pour s'assurer que chaque étape se termine avant que la suivante ne se déclenche. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint conforme [JSON:API](https://jsonapi.org) auto-généré pour chaque table enregistrée. Même RLS, masquage, et routage que GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**En-tête `Accept` :** doit inclure `application/vnd.api+json` (le type de média JSON:API) ou la requête retourne `406`.

**Paramètres de requête :**

- `fields[<type>]` — jeux de champs épars, ex. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — ex. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — séparé par des virgules, préfixe `-` pour descendant, ex. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — pagination
- `aggregate` — fonctions d'agrégation séparées par des virgules à exécuter au lieu de la récupération de lignes : `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Utilisez `?aggregate=count,sum` pour demander un sous-ensemble. Les réponses d'agrégation retournent `data: null` avec les résultats dans `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — noms de colonne séparés par des virgules ; utilisé avec `?aggregate=` pour grouper les résultats. Seules les colonnes dans l'enum `DistinctOnColumn` de la table sont valides ; le serveur retourne `400` pour toute colonne que le rôle ne peut pas voir. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` pour inclure les colonnes scalaires de la table de base (et les scalaires de dimension jointe nommés dans `include=`) dans le tableau `nodes` de chaque ligne de groupe. Requis quand une requête NL de groupement demande aussi des détails de dimension. (REQ-1405)

Les réponses sont des objets ressource avec `type`/`id`/`attributes`. Les erreurs suivent la forme d'objet d'erreur JSON:API.

#### Explorateur JSON:API

La page explorateur JSON:API (`/app/jsonapi`) est une UI navigateur sur ces endpoints. Sélectionnez une table depuis la liste groupée par domaine, puis configurez :

- **Champs** — choisissez quelles colonnes inclure (jeu de champs épars) ; laissez tout décoché pour demander chaque colonne
- **Relations** — sélectionnez les noms de relation dérivés de FK à charger via `?include=`
- **Filtre** — champ, opérateur (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`), et valeur
- **Tri** — un champ, ascendant ou descendant
- **Agrégation** — choisissez des colonnes de groupement depuis la liste validée par le serveur, puis cochez une ou plusieurs fonctions d'agrégation ; quand des colonnes de groupement sont sélectionnées, une case « Inclure les nœuds » ajoute les colonnes scalaires de la table de base à chaque ligne
- **Taille de page** — ressources par page, avec navigation premier/précédent/suivant/dernier

Les résultats s'affichent dans une vue résumé formatée (cartes de ressource avec ancres de relation cliquables) ou un onglet JSON brut. L'URL de la requête en direct est affichée et peut être copiée. La sélection de table et la taille de page persistent entre sessions dans `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

Quand elle est atteinte depuis un lien NL « Ouvrir dans JSON:API », l'explorateur pré-sélectionne la table et amorce le sélecteur d'agrégation depuis les paramètres de requête générés par NL, puis exécute automatiquement la requête. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Soumet une question en langage naturel. Le service démarre un job asynchrone et retourne `202 Accepted` avec un `job_id` immédiatement. Nécessite un fournisseur LLM configuré sous la section de config `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Corps de requête :**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Retourne `{"job_id": "<id>"}`. Dépasser la limite de débit NL par rôle retourne `429` avec un en-tête `Retry-After`. (REQ-370)

**Récupérer le résultat :**

- `GET /query/nl/{job_id}` — polling. Retourne le document de job.
- `GET /query/nl/{job_id}/stream` — SSE. Un événement `branch` par cible de génération à mesure qu'elle se termine, puis un événement `done`. (REQ-357, REQ-358)

Trois boucles de génération (Cypher, GraphQL, SQL) s'exécutent en parallèle, chacune validée à travers le compilateur et affinée en cas d'erreur. (REQ-355) Le prompt est scopé au schéma visible du rôle. (REQ-356) Le document de résultat classe chaque branche par cible : (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Une branche qui épuise sa limite d'itération retourne `query: null`, `result: null`, et une chaîne `error`. Chaque requête générée s'exécute sous les droits du consommateur avec la gouvernance Stage 2 appliquée — le service ne contourne jamais la gouvernance. (REQ-359)

#### Groupement NL avec détails de dimension (REQ-1405)

Quand une requête NL de groupement projette aussi des colonnes depuis une table de dimension jointe — par exemple, « nombre de demandes par utilisateur avec nom et email de l'utilisateur » — l'exécuteur dérive des chemins pointés par champ (`dim_paths`) depuis les colonnes de dimension projetées dans le SELECT. Ces chemins peuplent le paramètre `includeNodes=` sur les URL générées des panneaux JSON:API et OpenAPI, de sorte que ces panneaux demandent les mêmes champs de dimension jointe que les branches SQL et GraphQL ont résolus. Sans cela, `includeNodes=true` ne retournerait que les propres champs scalaires de la table d'agrégation de base. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

Sur le panneau gRPC, le `{Type}GroupByRequest` généré porte `include_nodes` (bool) et `include` (chaîne répétée de noms de champ de relation). Le `{Type}GroupByRow` retourné inclut un champ `nodes` typé avec les lignes de détail de dimension. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Retourne le SDL GraphQL pour le schéma d'un rôle. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**En-têtes :** `X-Role: <role_id>` (requis)

**Paramètres de requête :**

- `domain` — ID de domaine séparés par des virgules. Quand défini, la réponse est filtrée au(x) domaine(s) nommé(s) et aux tables accessibles depuis eux.

**Réponse :** SDL GraphQL `text/plain`.

---

### `GET /data/introspection`

Retourne le JSON d'introspection GraphQL, optionnellement filtré par domaine. [tool-verified: `provisa/api/data/sdl.py:200`]

**En-têtes :** `X-Provisa-Role: <role_id>` (requis)

**Paramètres de requête :** `domain` — ID de domaine séparés par des virgules.

**Réponse :** résultat d'introspection `application/json`.

---

### `GET /data/graph-schema`

Retourne la vue graphe du schéma du rôle : labels de nœud et leurs types de relation, pour les clients Cypher/graphe. Inclut `pk_columns` par label de nœud pour que les appelants puissent déterminer les colonnes de clé primaire. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Réponse :** `application/json` avec `node_labels` (chacun portant `pk`/`pk_columns`) et `relationship_types`.

---

### `GET /data/domains`

Retourne les ID de domaine accessibles au rôle demandeur. [tool-verified: `provisa/api/data/sdl.py:116`]

**En-têtes :** `X-Role: <role_id>` (requis)

**Réponse :** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Retourne la chaîne de version de schéma actuelle. Combine un nonce par démarrage avec un compteur de reconstruction. Les clients utilisent ceci pour invalider les caches de schéma après les redémarrages serveur. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Réponse :** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Retourne le fichier `.proto` auto-généré pour un rôle. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Réponse :** schéma protobuf `text/plain`.

Chaque table enregistrée produit un `message` proto. Les relations produisent des champs de message imbriqués. Mapping de type : `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Flux Server-Sent Events pour les notifications de changement en temps réel depuis une table. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

La livraison de notification utilise un fournisseur enfichable choisi par type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY` (via asyncpg), les sources MongoDB utilisent Change Streams (`collection.watch()`), et les sources Kafka utilisent des groupes de consommateurs. Chaque fournisseur implémente une interface de veille async commune. Le filtrage RLS et la validation de schéma s'appliquent quel que soit le fournisseur. (REQ-258) Les sources WebSocket et RSS sont aussi prises en charge. (REQ-338, REQ-342)

**En-tête — `X-Provisa-Sink` :** Définissez sur une cible Kafka (ex. `kafka://broker:9092/topic`) pour rediriger les événements de changement vers un sink Kafka au lieu de la réponse SSE. Le serveur lance un consommateur sink et retourne `202 Accepted` plutôt qu'un flux ouvert. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Endpoints REST Admin

### Config

#### `GET /admin/config`

Télécharge le `provisa.yaml` actuel en `application/x-yaml` avec un en-tête `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Téléverse un YAML de config révisé. Le serveur écrit une sauvegarde `.bak`, enregistre le nouveau fichier, et recharge tous les schémas, sources, et vues matérialisées. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Corps de requête :** contenu YAML brut.

**Réponse :**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

En cas d'échec de rechargement : `{"success": false, "message": "<error>"}`.

---

### Paramètres

#### `GET /admin/settings`

Retourne les paramètres de plateforme actuels en JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

**Corps de requête (exemple partiel) :**

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

Champs mettables à jour par section :

- `redirect` : `enabled`, `threshold`, `default_format`, `ttl`
- `sampling` : `default_sample_size`
- `cache` : `default_ttl`
- `naming` : `domain_prefix`, `convention` — écrit dans le fichier de config et déclenche un rechargement de schéma (REQ-253)
- `relationships` : `auto_track_fk`
- `otel` : `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Réponse :**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Observabilité

#### `GET /admin/traces/recent`

Retourne jusqu'à N spans terminés récents depuis le buffer de spans en mémoire. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Paramètres de requête :** `limit` (défaut 50, max 200)

**Réponse :** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Recharge à chaud un catalogue nommé dans le coordinateur du moteur de fédération via son API REST. Reconnecte la connexion interne de Provisa et ré-exécute le DDL OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Paramètres de requête :** `catalog` (défaut `"otel"`)

**Réponse :**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Redémarre le conteneur du moteur de fédération (dev mono-nœud uniquement). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Paramètres de requête :** `container` (défaut : variable d'env `QUERY_ENGINE_CONTAINER`, puis `"trino"`)

---

### Découverte

#### `POST /admin/discover/relationships`

Déclenche la découverte de relations. Exécute toujours l'introspection FK depuis le moteur de fédération. (REQ-018) Exécute l'inférence LLM si `ANTHROPIC_API_KEY` est défini. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Corps de requête :**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` doit être l'un de `"table"`, `"domain"`, `"cross-domain"`. Pour le scope `"table"`, `table_id` (entier) est requis. Pour le scope `"domain"`, `domain_id` est requis.

**Réponse :** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Liste les candidats de relation en attente. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Accepte un candidat et l'enregistre comme relation. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Corps de requête (optionnel) :** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rejette un candidat. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Corps de requête :** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Retourne le nombre de candidats rejetés. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Supprime tous les candidats rejetés. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Crawl de source

#### `POST /admin/sources/crawl`

Parcourt une source de données pour introspecter son schéma et enregistrer les tables. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Recherche de table de source

#### `GET /admin/sources/{source_id}/tables/search`

Recherche les tables disponibles (pas encore enregistrées) dans une source par nom. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Profilage de table

#### `POST /admin/tables/{table_id}/profile`

Exécute un profil de colonne sur une table enregistrée — cardinalité, min/max, taux de nul. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Descriptions de source

#### `POST /admin/source-meta/db-description`

Génère des descriptions assistées par LLM pour les tables et colonnes d'une source. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Actions (fonctions et webhooks)

Tous les endpoints sont sous le préfixe `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Chaque invocation — depuis GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql`, et Provisa gRPC — passe par un unique exécuteur gouverné qui applique `writable_by` et la gouvernance uniformément. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Voir [docs/integrations.md](integrations.md#invoquer-des-commandes-a-travers-les-protocoles) pour la syntaxe d'appel par protocole.

#### `GET /admin/actions`

Retourne toutes les fonctions DB et webhooks suivis. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

Chaque objet webhook porte un booléen `approved`. Un webhook est approuvé une fois qu'un steward exécute sa demande de création (REQ-209) ; les webhooks déclarés en config sont auto-approuvés. Un webhook non approuvé est enregistré mais n'est exposé sur aucune surface. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Enregistre une fonction suivie (commande). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Champs clés :**

| Champ | Requis | Description |
| --- | --- | --- |
| `name` | Oui | Nom de commande unique |
| `kind` | Oui | `"query"` → champ GraphQL Query ; `"mutation"` → champ Mutation |
| `implKind` | Non | Comment la commande s'exécute — voir le tableau ci-dessous (défaut `source_procedure`) |
| `binding` | Non | Détails de connexion spécifiques à `implKind` (objet JSON) |
| `returnSchema` | Non | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — rend la commande set-returning sur chaque surface |
| `arguments` | Non | Définitions d'argument `[{name, type}]` ; l'ordre positionnel compte pour les appelants SQL et Bolt |
| `visibleTo` | Non | ID de rôle pouvant appeler la commande |
| `writableBy` | Non | ID de rôle autorisés à l'invoquer comme mutation |
| `domainId` | Non | Domaine pour le placement GraphQL et le contrôle d'accès |

**Valeurs `implKind` :**

| `implKind` | Ce qui s'exécute | Champs `binding` |
| --- | --- | --- |
| `source_procedure` | Procédure stockée sur une source enregistrée (défaut) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script côté serveur | `script` |
| `http` | Appel HTTP sortant | `url`, `method` |
| `grpc` | Appel gRPC sortant vers un serveur externe | `target`, `method` |
| `python` | Callable Python hébergé par Provisa (REQ-885) | `callable` (ex. `"demo.py_functions:random_dataset"`) |

Les commandes de démo `random_python_set` (`implKind: python`) et `random_grpc_set` (`implKind: grpc`) montrent des commandes set-returning avec `returnSchema` en pratique ; les deux sont dans `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Met à jour une fonction suivie par nom. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Supprime une fonction suivie par nom. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Enregistre un webhook suivi. (REQ-209) Enregistrer ou mettre à jour un webhook met en file une demande d'approbation steward — le webhook devient actif sur toutes les surfaces seulement après approbation par un steward. Les webhooks déclarés en config sont auto-approuvés. **Champs du corps de requête :** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Met à jour un webhook suivi par nom. Toute modification remet l'approbation en attente jusqu'à ré-approbation. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Supprime un webhook suivi par nom. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Teste une action (fonction ou webhook) par nom. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Rôles

Tous les endpoints sont sous le préfixe `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Liste tous les rôles |
| `POST` | `/admin/roles/` | Crée un rôle |
| `PUT` | `/admin/roles/{role_id}` | Met à jour un rôle |
| `DELETE` | `/admin/roles/{role_id}` | Supprime un rôle |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Utilisateurs

Tous les endpoints sont sous le préfixe `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `POST` | `/admin/users/` | Crée un utilisateur local |
| `GET` | `/admin/users/` | Liste les utilisateurs locaux |
| `GET` | `/admin/users/{user_id}` | Récupère un utilisateur |
| `PUT` | `/admin/users/{user_id}` | Met à jour un utilisateur |
| `PATCH` | `/admin/users/{user_id}/password` | Change le mot de passe |
| `DELETE` | `/admin/users/{user_id}` | Supprime un utilisateur |
| `GET` | `/admin/users/{user_id}/assignments` | Liste les affectations de rôle |
| `POST` | `/admin/users/{user_id}/assignments` | Ajoute une affectation de rôle |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Retire une affectation de rôle |

---

### Organisations

Tous les endpoints sont sous `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Liste les orgs |
| `POST` | `/admin/orgs/` | Crée une org |
| `PUT` | `/admin/orgs/{org_id}` | Met à jour une org |
| `DELETE` | `/admin/orgs/{org_id}` | Supprime une org |
| `GET` | `/admin/orgs/{org_id}/members` | Liste les membres |
| `POST` | `/admin/orgs/{org_id}/members` | Ajoute un membre |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Retire un membre |

---

### Invitations

Tous les endpoints sont sous `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Crée une invitation |
| `GET` | `/admin/invites/` | Liste les invitations en attente |
| `DELETE` | `/admin/invites/{token}` | Révoque une invitation |

---

### GraphQL Admin

#### `POST /admin/graphql`

Endpoint GraphQL Strawberry pour toutes les opérations admin : CRUD source et table, gestion de relation, configuration de domaine, règles RLS, contrôle de cache, conventions de nommage, gestion de tâches planifiées, et compilation de requête. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

Retourne le statut de configuration au premier lancement. Toujours non authentifié. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Termine la configuration au premier lancement. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Vérification de santé

#### `GET /health` ou `HEAD /health`

Retourne `{"status": "ok"}`. Toujours non authentifié. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Réponses d'erreur

| Statut | Signification |
| --- | --- |
| 400 | Requête invalide, erreur de validation, ou erreur de parsing SQL |
| 401 | Jeton d'auth manquant ou invalide |
| 403 | Capacités insuffisantes ; violation de gouvernance |
| 404 | Rôle, ressource, ou fichier de config non trouvé |
| 422 | En-tête requis manquant (ex. `X-Role`) |
| 503 | Base de données ou source non connectée ; dépendance indisponible |
| 504 | Requête expirée |

Les violations de gouvernance sur `POST /data/sql` retournent HTTP 403 avec un corps structuré : (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

## Endpoint Arrow Flight

Port `8815`. Transport columnaire Arrow natif sur gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Les requêtes et la découverte de catalogue sont toutes deux disponibles sur la même connexion. Le pipeline de gouvernance complet (RLS, masquage, échantillonnage) est appliqué à chaque requête. (REQ-130, REQ-143)

**Format de ticket** (JSON) :

```json
{"query": "{ customers { name email } }", "role": "analyst", "variables": {}}
```

**Usage (Python) :**

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

Quand le proxy Flight SQL Zaychik est disponible (port 8480), les lots d'enregistrements sont streamés de bout en bout sans matérialisation complète. (REQ-144) Se replie sur la matérialisation via la couche de requête fédérée si Zaychik n'est pas disponible. (REQ-146)

---

## Endpoint gRPC Protobuf

Port `50051` (surchargez avec la variable d'env `GRPC_PORT` ou la config `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Passez le rôle dans la clé de métadonnées gRPC `x-provisa-role`. Si absente, le serveur avorte avec `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Téléchargez le proto spécifique au rôle depuis `GET /data/proto/{role_id}`. Seules les tables et colonnes visibles pour ce rôle apparaissent. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Chaque table produit un RPC streaming `Query{TypeName}`. Les RPC `Insert{TypeName}` existent pour la symétrie de schéma mais avortent avec `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` est activé pour la découverte de service sans proto pré-compilé. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Le serveur gRPC ne démarre que quand un proto valide peut être compilé au démarrage. Si la construction du schéma échoue, le serveur gRPC ne démarre pas. (REQ-529)

#### RPC d'agrégation et de groupement (REQ-1359, REQ-1361, REQ-1405)

Quand une table a `enable_aggregates` défini, le proto généré inclut deux RPC supplémentaires aux côtés de `Query{TypeName}` :

- **`Query{TypeName}Aggregate`** — retourne des scalaires d'agrégation pour la table (`count` ; `sum`, `avg`, `stddev`, `variance` par colonne numérique ; `min`, `max` par colonne comparable)
- **`Query{TypeName}GroupBy`** — retourne une ligne par clé de groupe avec des sous-champs d'agrégation et, optionnellement, des scalaires de table de base et des lignes de dimension jointe dans un champ `nodes`

Les deux passent par le même pipeline d'agrégation du compilateur que les champs racine GraphQL `{field}_aggregate` et `{field}_group_by` — pas d'implémentation d'agrégation séparée. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Champ `funcs` (REQ-1361).** Le message de requête accepte un champ chaîne répétée `funcs`. Les valeurs valides sont `count`, `sum`, `avg`, `stddev`, `variance`, `min`, et `max`. Quand `funcs` est omis, chaque fonction que le schéma expose pour cette table est demandée. Quand défini, seules les fonctions nommées apparaissent. Si aucune des fonctions nommées ne s'applique aux types de colonne de la table, la requête se replie sur `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Champs `include_nodes` et `include` (REQ-1405).** Les requêtes `Query{TypeName}GroupBy` peuvent définir `include_nodes: true` pour inclure les colonnes scalaires de la table de base dans le champ `nodes` de chaque ligne. Le champ chaîne répétée `include` nomme les champs de relation many-to-one dont les colonnes scalaires sont aussi imbriquées dans `nodes`. Cela correspond au comportement `?includeNodes=` / `?include=` de JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Pilote JDBC

Le pilote JDBC Provisa (`provisa-jdbc-0.1.0.jar`) expose le catalogue sémantique aux outils BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL de connexion :** `jdbc:provisa://host:port` (REQ-131)

Les domaines se mappent vers des schémas JDBC. (REQ-127) Les tables utilisent leurs alias enregistrés. Les colonnes utilisent des alias et exposent les descriptions comme `REMARKS`. (REQ-128) Les méthodes de métadonnées standard (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exposent les relations sémantiques comme métadonnées PK/FK.

**Support SQL :** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Le pilote demande une redirection Arrow IPC par défaut. Les résultats sont streamés lot par lot via `ArrowStreamReader`, bornés à un lot d'enregistrements en mémoire. (REQ-293)

---

## Format de l'argument `orderBy`

L'argument `order_by` utilise des objets `{column: direction}` avec un enum de direction à 6 valeurs : (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Directions prises en charge : `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Abonnements

Les abonnements SSE sont disponibles à `GET /data/subscribe/{table}`. (REQ-219, REQ-258) La livraison de notification utilise un fournisseur enfichable sélectionné par type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY`, les sources MongoDB utilisent Change Streams, et les sources Kafka utilisent des groupes de consommateurs. Le filtrage RLS et la validation de schéma s'appliquent quel que soit le fournisseur. Les sources WebSocket et RSS sont aussi prises en charge via le même endpoint. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Glossaire métier (REQ-1387)

Le glossaire métier mappe les noms de champ physiques — tels qu'ils existent dans les bases de données source — vers un vocabulaire humain partagé. Chaque colonne enregistrée dans la couche sémantique reçoit automatiquement un terme. Aucune saisie manuelle n'est requise pour peupler le glossaire ; les curateurs ajoutent des définitions, relations, et experts par-dessus ce que le système dérive.

### Comment les termes sont dérivés

Quand Provisa enregistre ou met à jour les colonnes d'une table, `normalize_term` (`provisa/core/glossary.py`) s'exécute sur chaque nom de colonne et produit une phrase canonique. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

La normalisation applique cinq règles en séquence :

1. Découper sur les frontières camelCase et les caractères séparateurs (`_`, `-`, `.`, `/`, espace).
2. Mettre le résultat en minuscules.
3. Étendre une table d'abréviations fixe (ex. `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Retirer un **jeton proxy** final (`identifier`, `code`, `index`, ou `reference`) — une colonne nommée d'après sa clé ou son code pointe vers le concept sous-jacent à travers une valeur de substitution, donc le terme devrait être le concept lui-même. Le dernier jeton restant n'est jamais retiré.
5. Qualifier une **phrase trop générique** avec le concept de la table. Quand la phrase normalisée complète est un mot d'attribut nu (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, et similaires), le terme devient `<concept de table> <phrase>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Un seul terme `name` partagé entre tables non liées fusionnerait des significations distinctes ; la qualification relie chaque colonne à son concept englobant à la place. Le concept de table est le nom métier de la table, normalisé avec un nom noyau au singulier (`order_lines` → `order line`).

Les pseudo-colonnes de filtre natif (préfixées `_nf_`, ou toute colonne portant `native_filter_type`) sont de la mécanique de paramètre de requête, pas des champs métier, et ne dérivent aucun terme.

Parce que `id`, `key`, `pk`, et `sk` s'étendent tous vers `identifier` avant la vérification proxy, trois noms de colonne physiquement différents atterrissent exactement sur le même terme :

| Nom physique | Après normalisation |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Les trois premiers s'effondrent en un seul terme. `transaction amount` garde les deux jetons car `amount` n'est pas un proxy. Une colonne `id` nue — sans jetons précédents — ne peut pas être retirée ; elle se normalise en `identifier` de sorte que le terme ne soit pas vide. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Cycle de vie

Les termes sont **dérivés de l'appartenance à la couche sémantique**, pas créés à la demande par les utilisateurs. Le dépôt de table est le chemin d'écriture unique : `sync_table_refs` s'exécute dans chaque upsert de jeu de colonnes, et `sweep_refless_terms` s'exécute après tout chemin de suppression. [tool-verified: `provisa/core/repositories/glossary.py`]

**Quand une colonne est ajoutée :** Provisa recherche le terme normalisé par nom. S'il existe déjà, la colonne reçoit une référence vers lui (et si le terme était déprécié, il est ravivé — `deprecated` est remis à `False`). Si aucun terme n'existe encore, un est créé.

**Quand une colonne part** (changement de schéma ou suppression de table) : sa référence est supprimée et le terme est **réglé** sous une règle de suppression-ou-dépréciation. Un terme enraciné sans référence restante est supprimé purement — avec ses arêtes et affectations d'experts — sauf si le supprimer laisserait un terme abstrait déconnecté de tous les termes enracinés (aucun chemin à travers le graphe de termes). Dans ce cas, le terme est **déprécié** (marqué `deprecated=True`) plutôt que supprimé, de sorte que l'ancrage de graphe du terme abstrait survive.

Les termes abstraits ne sont jamais auto-supprimés ; ils existent en dehors du cycle de vie physique et ne sont supprimés qu'explicitement via l'API admin.

**Réanimation :** si le nom normalisé d'un terme déprécié réapparaît (une colonne est ré-enregistrée), le terme est démarqué et ses références reprennent leur accumulation.

### Endpoints de curation

Tous les endpoints sont sous `/admin/glossary`. Ils nécessitent un accès `org_admin` et une org configurée. Chaque mutation déclenche une publication de métadonnées. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Liste les termes. Paramètres de requête : `q` (recherche nom/définition), `include_deprecated` (défaut `true`) |
| `GET` | `/admin/glossary/terms/{term_id}` | Récupère le détail d'un terme : définition, références physiques, arêtes typées, experts |
| `POST` | `/admin/glossary/terms` | Crée un terme abstrait — vocabulaire utilisateur sans référence physique |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Renomme, définit la définition, ou bascule l'exclusion d'export |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Supprime un terme sans référence physique |
| `POST` | `/admin/glossary/refs/move` | Déplace une référence physique vers un autre terme (consolidation) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Ajoute une arête de relation typée entre deux termes |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Retire une arête (paramètres de requête : `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Marque un utilisateur comme expert ou auteur pour un terme |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Retire la désignation expert/auteur d'un utilisateur |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Rédige une définition pour un terme en utilisant le modèle IA de l'org — retourne du texte uniquement, rien ne persiste avant enregistrement |
| `POST` | `/admin/glossary/definitions/generate` | Génère et persiste des définitions pour chaque terme qui n'en a aucune — n'écrase jamais un texte rédigé par un humain |
| `POST` | `/admin/glossary/relationships/generate` | Propose et persiste des arêtes typées à travers tout le glossaire en utilisant le modèle IA de l'org |

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

Déplacer une référence règle le terme perdant sous la règle de suppression-ou-dépréciation. Utilisez ceci pour consolider deux termes que la normalisation a gardés séparés — par exemple, après qu'une source utilise une abréviation non standard tombée en dehors de la table d'expansion.

Supprimer un terme enraciné (avec des références physiques) retourne `400 glossary.invalid`. Retirez ou déplacez d'abord toutes les références.

**Champ `export_excluded` de `PATCH /admin/glossary/terms/{term_id}` :**

```json
{"export_excluded": true}
```

Définir `export_excluded` à `true` retient le terme de tous les instantanés d'export de métadonnées, indépendamment de ses références physiques ou de son statut abstrait. Le remettre à `false` restaure le terme dans l'instantané à la prochaine publication. Les données de curation (définition, arêtes, experts) ne sont pas affectées. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Curation assistée par IA

Le modèle IA configuré de l'org peut rédiger des définitions et proposer des arêtes de relation à travers tout le glossaire en une opération. Les deux actions en masse nécessitent un accès `org_admin` et une org configurée.

**`POST /admin/glossary/definitions/generate`**

Itère sur chaque terme du glossaire, saute ceux qui ont déjà une définition, et appelle le modèle IA de l'org pour en rédiger une pour chaque terme restant. Le brouillon est persisté immédiatement — contrairement à l'endpoint de brouillon par terme (`POST /admin/glossary/terms/{term_id}/definition/generate`), il n'y a pas d'étape d'édition. Les définitions rédigées par des humains ne sont jamais écrasées : la garde est `if summary["definition"]: continue` avant tout appel modèle. Une notification de publication couvre le lot entier. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Réponse :

```json
{"generated": 12}
```

`generated` est le nombre de termes ayant reçu une nouvelle définition. Il vaut zéro quand chaque terme en a déjà une.

**`POST /admin/glossary/relationships/generate`**

Envoie la liste complète des termes au modèle IA de l'org avec un prompt qui spécifie les dix types d'arête autorisés (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) et demande uniquement des propositions sûres. Le modèle répond avec un tableau JSON ; chaque entrée est validée avant toute écriture : les noms de terme inconnus, les auto-arêtes, et les types d'arête hors de l'enum fermé sont silencieusement rejetés. Les propositions valides sont upsertées de manière idempotente — relancer l'action ne duplique pas les arêtes. Une notification de publication couvre le lot. L'endpoint retourne `{"added": 0}` immédiatement quand le glossaire contient moins de deux termes non dépréciés. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Réponse :

```json
{"added": 5}
```

`added` est le nombre d'arêtes écrites. Une arête qui existait déjà compte quand même — l'upsert réussit, mais les données de l'arête ne changent pas.

### Outil MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Recherche les noms et définitions de terme avec une correspondance de sous-chaîne insensible à la casse, jusqu'à `limit` résultats. Chaque résultat est le détail complet du terme : `name`, `definition`, `is_abstract`, `deprecated`, références physiques (avec `source_id`, `schema_name`, `table_name`, `column_name`), arêtes typées, et affectations d'experts. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Utilisez `search_terms` avant d'écrire du SQL pour trouver chaque champ physique représentant un concept par son nom. Par exemple, rechercher `"order date"` retourne le terme et toutes les colonnes `order_dt`, `orderDate`, `ORDER_DATE` à travers chaque table enregistrée.

### Export de métadonnées

Le graphe de termes du glossaire est inclus dans chaque `MetadataSnapshot` construit par `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

L'export applique les mêmes filtres que le reste de l'instantané :

- Un terme marqué `export_excluded` est retenu purement — indépendamment de ses références physiques, de son statut abstrait, ou du fait que le catalogue de l'org soit configuré. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Un terme enraciné ne publie que quand au moins une de ses références physiques appartient à une colonne qui passe à la fois le filtre **Data Product** (le drapeau `data_product` de la table doit être `true`) et le filtre de colonne **technique** (les colonnes marquées `technical` sont retenues).
- Un terme enraciné dont toutes les références sont retenues par ces filtres est retenu avec elles.
- Les termes abstraits publient inconditionnellement — ce sont du vocabulaire utilisateur, non liés à des colonnes physiques.
- Une arête entre deux termes ne publie que quand les deux termes aux extrémités publient.

Chaque adaptateur vendeur publie le graphe de termes nativement, dans un conteneur de glossaire propriété de Provisa qu'il crée de manière idempotente — jamais dans un glossaire de catalogue existant :

| Fournisseur | Conteneur | Termes | Relations | Dépréciation |
| --- | --- | --- | --- | --- |
| Apache Atlas | « Provisa Glossary » (API glossaire) | termes de glossaire, définition sur `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | marqueur shortDescription `[DEPRECATED]` |
| Atlan | Glossaire Provisa par qualifiedName stable | `longDescription` (jamais le `userDescription` édité par un humain) | même mapping Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | aspect `glossaryTermInfo` par terme | KIND_OF → Inherits, PART_OF → Contains (inversé), RELATED_TO/SYNONYM_OF → termes liés | aspect de dépréciation ; les renommages suivent la succession URN |
| OpenMetadata | Glossaire Provisa via `/v1/glossaries` | PUT clé par fqn, renommages re-liaison PATCH par UUID stocké | KIND_OF → hiérarchie parent native, SYNONYM_OF → `synonyms`, autres → `relatedTerms` | `entityStatus` |
| Collibra | Domaine de type glossaire « Provisa Glossary » | Actifs Business Term via l'API Import | types de relation Business Term natifs | statut d'actif |

La propriété est le lien de rattachement, pas le nom : l'id vendeur de chaque terme publié est capturé dans `catalog_bindings` sous l'URN du terme (`provisa://<org>/terms/<name>`), et Provisa ne modifie ou supprime un élément de glossaire côté vendeur que quand il détient ce lien (ou que l'élément vit dans le conteneur propriété de Provisa qu'il a créé). Un élément de glossaire sans lien Provisa provient du système externe et n'est jamais touché ; les mises à jour font une lecture-fusion de sorte que les champs ajoutés par un steward sur les propres termes de Provisa survivent ; rien n'est supprimé quand un terme quitte l'instantané. Les affectations terme-vers-actif par les stewards restent la propriété de l'externe — aucun adaptateur n'écrit d'affectations terme-vers-actif (la publication d'affectations rédigées par Provisa est un suivi explicite). Sur Collibra spécifiquement, la sécurité sous la sémantique REPLACE de l'API Import repose sur le confinement : le payload ne mentionne que des actifs à l'intérieur du domaine de glossaire Provisa et des instances de relation uniquement entre termes Provisa, de sorte que les glossaires des stewards et leurs relations ne sont jamais atteignables. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
