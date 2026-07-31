# Référence de l'API

## Vue d'ensemble

Provisa expose des endpoints REST sous deux préfixes : `/data` pour l'exécution de requêtes et l'introspection de schéma, et `/admin` pour la gestion de la configuration. (REQ-043) La plupart des endpoints de données exigent un identifiant de rôle. Les opérations de configuration d'administration utilisent une API Strawberry GraphQL sur `/admin/graphql`. (REQ-164)

---

## Authentification

Lorsque `auth.provider` est configuré dans `provisa.yaml`, tous les endpoints à l'exception de `/health` et `/setup/status` exigent un en-tête `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Sans authentification configurée, le serveur s'exécute en mode développement. Toute requête est traitée comme l'identité `anonymous`, qui est associée à tous les rôles configurés avec un accès de domaine générique. (REQ-535)

**Connexion (`POST /auth/login`)** est fournie par le fournisseur d'authentification actif lorsque `provider: basic` est configuré. (REQ-124) Le format des identifiants et la réponse dépendent du fournisseur.

**Introspection d'identité :**

```http
GET /auth/me
```

Renvoie l'id, l'adresse e-mail, le nom d'affichage, les appartenances aux organisations et les affectations de rôle de l'utilisateur authentifié. En mode développement, renvoie `dev_mode: true` avec la liste de tous les ID de rôle. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Renvoie `{"provider": "<name>"}` ou `{"provider": null}` lorsque l'authentification n'est pas configurée. [tool-verified: `provisa/api/auth_router.py`]

---

## Endpoints de données

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

Le champ `role` n'est utilisé qu'en mode développement (sans authentification). Lorsque l'authentification est active, le rôle de l'utilisateur authentifié est utilisé et le `role` du corps de la requête est ignoré.

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

**Réponse (racines multiples, mélange en ligne/redirection) :**

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

Les requêtes à racines multiples exécutent chaque champ racine indépendamment. Les champs sous le seuil de redirection sont renvoyés en ligne ; ceux au-dessus sont redirigés. La clé `redirects` (pluriel) associe les noms de champ aux informations de redirection. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**En-têtes de cache :**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (en cas de HIT) (REQ-536)

**Capacités requises :** `QUERY_DEVELOPMENT` pour toutes les requêtes, y compris l'introspection. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Négociation de contenu

| En-tête Accept | Format |
| --- | --- |
| `application/json` | JSON (par défaut) |
| `application/x-ndjson` | JSON délimité par saut de ligne |
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
  "role": "admin",
  "discovery_mode": false
}
```

L'indicateur `discovery_mode` élargit la vérification de visibilité des tables à toutes les tables de tous les contextes. Réservé aux outils internes. [tool-verified: `provisa/api/data/endpoint_dev.py:148-152`]

**Capacités requises :** `QUERY_DEVELOPMENT`.

Les violations de gouvernance sur `POST /data/sql` renvoient un code HTTP 403. (REQ-002, REQ-266)

**Réponse :** Même format que `/data/graphql` (lignes JSON par défaut, négociées par contenu via `Accept`).

---

### `POST /data/query`

Endpoint de requête unifié. Accepte GraphQL, SQL ou Cypher — la syntaxe est détectée automatiquement. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Les requêtes Cypher peuvent également être envoyées à l'endpoint exclusivement Cypher `POST /query/cypher`. (REQ-345)

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

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST simple généré automatiquement pour chaque table enregistrée. La chaîne de requête est mappée sur des arguments GraphQL et la requête est compilée et exécutée à travers le même pipeline (RLS, masquage, routage) que GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Paramètres de requête :**

- `limit` — nombre maximal de lignes (≥ 1)
- `offset` — nombre de lignes à ignorer (≥ 0)
- `fields` — noms de colonnes séparés par des virgules (par défaut, tous les champs scalaires)
- `filter` — tableau JSON d'objets de filtre `{"field", "comparator", "value"}`
- `orderBy` — tableau JSON d'objets de tri `{"field", "direction"}`

Le rôle authentifié est requis ; les requêtes non authentifiées renvoient `401`. Une spécification OpenAPI pour ces routes est fournie sur `GET /data/rest/openapi.json`, avec une interface Swagger UI sur `GET /data/rest/docs`.

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint conforme à [JSON:API](https://jsonapi.org) généré automatiquement pour chaque table enregistrée. Mêmes RLS, masquage et routage que GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**En-tête `Accept` :** doit inclure `application/vnd.api+json` (le type de média JSON:API), sinon la requête renvoie `406`.

**Paramètres de requête :**

- `fields[<type>]` — ensembles de champs partiels (sparse fieldsets), p. ex. `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — p. ex. `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — séparé par des virgules, préfixe `-` pour un ordre décroissant, p. ex. `?sort=-created_at,amount`
- `page[number]` / `page[size]` — pagination

Les réponses sont des objets ressource avec `type`/`id`/`attributes`. Les erreurs suivent la structure d'objet d'erreur JSON:API.

---

### `POST /query/nl`

Soumet une question en langage naturel. Le service démarre une tâche asynchrone et renvoie immédiatement `202 Accepted` avec un `job_id`. Nécessite un fournisseur de LLM configuré dans la section de configuration `ai_models`. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Corps de la requête :**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Renvoie `{"job_id": "<id>"}`. Le dépassement de la limite de fréquence NL par rôle renvoie `429` avec un en-tête `Retry-After`. (REQ-370)

**Récupérer le résultat :**

- `GET /query/nl/{job_id}` — interrogation périodique (polling). Renvoie le document de la tâche.
- `GET /query/nl/{job_id}/stream` — SSE. Un évènement `branch` par cible de génération au fur et à mesure de son achèvement, puis un évènement `done`. (REQ-357, REQ-358)

Trois boucles de génération (Cypher, GraphQL, SQL) s'exécutent en parallèle, chacune validée par le compilateur et affinée en cas d'erreur. (REQ-355) Le prompt est limité au schéma visible du rôle. (REQ-356) Le document de résultat indexe chaque branche par cible : (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Une branche qui épuise sa limite d'itérations renvoie `query: null`, `result: null` et une chaîne `error`. Chaque requête générée s'exécute sous les droits du consommateur, avec la gouvernance de l'étape 2 appliquée — le service ne contourne jamais la gouvernance. (REQ-359)

---

### `GET /data/sdl`

Renvoie le SDL GraphQL du schéma d'un rôle. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**En-têtes :** `X-Role: <role_id>` (obligatoire)

**Paramètres de requête :**

- `domain` — ID de domaine séparés par des virgules. Lorsqu'il est défini, la réponse est filtrée sur le(s) domaine(s) indiqué(s) et les tables accessibles depuis ceux-ci.

**Réponse :** SDL GraphQL en `text/plain`.

---

### `GET /data/introspection`

Renvoie le JSON d'introspection GraphQL, filtré par domaine en option. [tool-verified: `provisa/api/data/sdl.py:200`]

**En-têtes :** `X-Provisa-Role: <role_id>` (obligatoire)

**Paramètres de requête :** `domain` — ID de domaine séparés par des virgules.

**Réponse :** résultat d'introspection en `application/json`.

---

### `GET /data/graph-schema`

Renvoie la vue en graphe du schéma du rôle : les étiquettes de nœud et leurs types de relation, pour les clients Cypher/graphe. Inclut `pk_columns` par étiquette de nœud afin que les appelants puissent déterminer les colonnes de clé primaire. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Réponse :** `application/json` avec `node_labels` (chacun portant `pk`/`pk_columns`) et `relationship_types`.

---

### `GET /data/domains`

Renvoie les ID de domaine accessibles au rôle demandeur. [tool-verified: `provisa/api/data/sdl.py:116`]

**En-têtes :** `X-Role: <role_id>` (obligatoire)

**Réponse :** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Renvoie la chaîne de version du schéma actuel. Combine un nonce par démarrage avec un compteur de reconstruction. Les clients l'utilisent pour invalider les caches de schéma après un redémarrage du serveur. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Réponse :** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Renvoie le fichier `.proto` généré automatiquement pour un rôle. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Réponse :** schéma protobuf en `text/plain`.

Chaque table enregistrée produit un `message` proto. Les relations produisent des champs de message imbriqués. Correspondance de types : `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Flux Server-Sent Events pour les notifications de changement en temps réel d'une table. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

La livraison des notifications utilise un fournisseur enfichable choisi selon le type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY` (via asyncpg), les sources MongoDB utilisent les Change Streams (`collection.watch()`), et les sources Kafka utilisent des groupes de consommateurs. Chaque fournisseur implémente une interface d'observation asynchrone commune. Le filtrage RLS et la validation de schéma s'appliquent quel que soit le fournisseur. (REQ-258) Les sources WebSocket et RSS sont également prises en charge. (REQ-338, REQ-342)

**En-tête — `X-Provisa-Sink` :** Définissez-le sur une cible Kafka (p. ex. `kafka://broker:9092/topic`) pour rediriger les évènements de changement vers un sink Kafka au lieu de la réponse SSE. Le serveur démarre un consommateur de sink et renvoie `202 Accepted` plutôt qu'un flux ouvert. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Endpoints REST d'administration

### Config

#### `GET /admin/config`

Télécharge le `provisa.yaml` actuel en tant que `application/x-yaml`, avec un en-tête `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Envoie un YAML de configuration révisé. Le serveur écrit une sauvegarde `.bak`, enregistre le nouveau fichier et recharge tous les schémas, sources et vues matérialisées. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Corps de la requête :** Contenu YAML brut.

**Réponse :**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

En cas d'échec du rechargement : `{"success": false, "message": "<error>"}`.

---

### Paramètres (Settings)

#### `GET /admin/settings`

Renvoie les paramètres actuels de la plateforme au format JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Met à jour les paramètres de la plateforme à l'exécution. Tous les champs sont facultatifs — seules les clés présentes dans le corps sont mises à jour. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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
- `naming` : `domain_prefix`, `convention` — écrit dans le fichier de configuration et déclenche le rechargement du schéma (REQ-253)
- `relationships` : `auto_track_fk`
- `otel` : `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Réponse :**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Observabilité

#### `GET /admin/traces/recent`

Renvoie jusqu'à N spans terminés récemment depuis le tampon de spans en mémoire. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Paramètres de requête :** `limit` (50 par défaut, 200 au maximum)

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

Déclenche la découverte de relations. Exécute toujours l'introspection des clés étrangères depuis le moteur de fédération. (REQ-018) Exécute une inférence par LLM si `ANTHROPIC_API_KEY` est définie. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Corps de la requête :**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` doit être `"table"`, `"domain"` ou `"cross-domain"`. Pour la portée `"table"`, `table_id` (entier) est requis. Pour la portée `"domain"`, `domain_id` est requis.

**Réponse :** `{"candidates_found": 12, "stored_ids": [1, 2, 3, ...]}`

#### `GET /admin/discover/candidates`

Liste les candidats de relation en attente. [tool-verified: `provisa/api/admin/discovery.py:96`]

#### `POST /admin/discover/candidates/{candidate_id}/accept`

Accepte un candidat et l'enregistre comme relation. [tool-verified: `provisa/api/admin/discovery.py:103`]

**Corps de la requête (facultatif) :** `{"name": "custom-relationship-name"}`

#### `POST /admin/discover/candidates/{candidate_id}/reject`

Rejette un candidat. [tool-verified: `provisa/api/admin/discovery.py:110`]

**Corps de la requête :** `{"reason": "Not a real join"}`

#### `GET /admin/discover/candidates/rejected/count`

Renvoie le nombre de candidats rejetés. [tool-verified: `provisa/api/admin/discovery.py:118`]

#### `DELETE /admin/discover/candidates/rejected`

Supprime tous les candidats rejetés. [tool-verified: `provisa/api/admin/discovery.py:128`]

---

### Exploration des sources (Source Crawl)

#### `POST /admin/sources/crawl`

Explore une source de données pour introspecter son schéma et enregistrer les tables. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Recherche de tables source

#### `GET /admin/sources/{source_id}/tables/search`

Recherche par nom les tables disponibles (pas encore enregistrées) dans une source. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Profilage de tables

#### `POST /admin/tables/{table_id}/profile`

Exécute un profil de colonnes sur une table enregistrée — cardinalité, min/max, taux de valeurs nulles. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Descriptions de source

#### `POST /admin/source-meta/db-description`

Génère des descriptions assistées par LLM pour les tables et colonnes d'une source. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Actions (fonctions et webhooks)

Tous les endpoints se trouvent sous le préfixe `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Chaque invocation — depuis GraphQL, SQL, Cypher, Bolt, Arrow Flight, MCP `run_sql` et Provisa gRPC — passe par un exécuteur gouverné unique qui applique `writable_by` et la gouvernance de manière uniforme. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Voir [docs/integrations.md](integrations.md#invoquer-des-commandes-entre-protocoles) pour la syntaxe d'appel par protocole.

#### `GET /admin/actions`

Renvoie toutes les fonctions de BD et tous les webhooks suivis. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

Chaque objet webhook porte un booléen `approved`. Un webhook est approuvé dès qu'un steward exécute sa demande de création (REQ-209) ; les webhooks déclarés dans la configuration sont approuvés automatiquement. Un webhook non approuvé est enregistré mais n'est exposé sur aucune surface. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Enregistre une fonction suivie (commande). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Champs clés :**

| Champ | Obligatoire | Description |
| --- | --- | --- |
| `name` | Oui | Nom de commande unique |
| `kind` | Oui | `"query"` → champ Query GraphQL ; `"mutation"` → champ Mutation |
| `implKind` | Non | Mode d'exécution de la commande — voir tableau ci-dessous (par défaut `source_procedure`) |
| `binding` | Non | Détails de connexion spécifiques à `implKind` (objet JSON) |
| `returnSchema` | Non | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — rend la commande retournant un ensemble sur chaque surface |
| `arguments` | Non | Définitions d'argument `[{name, type}]` ; l'ordre positionnel compte pour les appelants SQL et Bolt |
| `visibleTo` | Non | ID de rôle pouvant appeler la commande |
| `writableBy` | Non | ID de rôle autorisés à l'invoquer comme mutation |
| `domainId` | Non | Domaine pour le placement GraphQL et le contrôle d'accès |

**Valeurs de `implKind` :**

| `implKind` | Ce qui s'exécute | Champs de `binding` |
| --- | --- | --- |
| `source_procedure` | Procédure stockée sur une source enregistrée (par défaut) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script côté serveur | `script` |
| `http` | Appel HTTP sortant | `url`, `method` |
| `grpc` | Appel gRPC sortant vers un serveur externe | `target`, `method` |
| `python` | Callable Python hébergé par Provisa (REQ-885) | `callable` (p. ex. `"demo.py_functions:random_dataset"`) |

Les commandes de démonstration `random_python_set` (`implKind: python`) et `random_grpc_set` (`implKind: grpc`) illustrent en pratique des commandes retournant un ensemble avec `returnSchema` ; les deux figurent dans `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Met à jour une fonction suivie par son nom. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Supprime une fonction suivie par son nom. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Enregistre un webhook suivi. (REQ-209) L'enregistrement ou la mise à jour d'un webhook place une demande d'approbation du steward dans la file — le webhook n'est actif sur toutes les surfaces qu'une fois approuvé par un steward. Les webhooks déclarés dans la configuration sont approuvés automatiquement. **Champs du corps de la requête :** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

#### `PUT /admin/actions/webhooks/{name}`

Met à jour un webhook suivi par son nom. Toute modification remet l'approbation en attente jusqu'à nouvelle approbation. [tool-verified: `provisa/api/admin/actions_router.py:306`]

#### `DELETE /admin/actions/webhooks/{name}`

Supprime un webhook suivi par son nom. [tool-verified: `provisa/api/admin/actions_router.py:355`]

#### `POST /admin/actions/test`

Teste une action (fonction ou webhook) par son nom. (REQ-245) [tool-verified: `provisa/api/admin/actions_router.py:384`]

---

### Rôles

Tous les endpoints se trouvent sous le préfixe `/admin/roles`. [tool-verified: `provisa/api/admin/roles_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/roles/` | Liste tous les rôles |
| `POST` | `/admin/roles/` | Crée un rôle |
| `PUT` | `/admin/roles/{role_id}` | Met à jour un rôle |
| `DELETE` | `/admin/roles/{role_id}` | Supprime un rôle |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Utilisateurs

Tous les endpoints se trouvent sous le préfixe `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

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

Tous les endpoints se trouvent sous `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Liste les organisations |
| `POST` | `/admin/orgs/` | Crée une organisation |
| `PUT` | `/admin/orgs/{org_id}` | Met à jour une organisation |
| `DELETE` | `/admin/orgs/{org_id}` | Supprime une organisation |
| `GET` | `/admin/orgs/{org_id}/members` | Liste les membres |
| `POST` | `/admin/orgs/{org_id}/members` | Ajoute un membre |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Retire un membre |

---

### Invitations

Tous les endpoints se trouvent sous `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Crée une invitation |
| `GET` | `/admin/invites/` | Liste les invitations en attente |
| `DELETE` | `/admin/invites/{token}` | Révoque une invitation |

---

### GraphQL d'administration

#### `POST /admin/graphql`

Endpoint Strawberry GraphQL pour toutes les opérations d'administration : CRUD des sources et tables, gestion des relations, configuration des domaines, règles RLS, contrôle du cache, conventions de nommage, gestion des tâches planifiées et compilation de requêtes. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

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

### Configuration initiale (Setup)

#### `GET /setup/status`

Renvoie l'état de la configuration de premier démarrage. Toujours sans authentification. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Termine la configuration de premier démarrage. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Vérification d'état (Health Check)

#### `GET /health` ou `HEAD /health`

Renvoie `{"status": "ok"}`. Toujours sans authentification. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Réponses d'erreur

| Statut | Signification |
| --- | --- |
| 400 | Requête invalide, erreur de validation ou erreur d'analyse SQL |
| 401 | Jeton d'authentification manquant ou invalide |
| 403 | Capacités insuffisantes ; violation de gouvernance |
| 404 | Rôle, ressource ou fichier de configuration introuvable |
| 422 | En-tête obligatoire manquant (p. ex. `X-Role`) |
| 503 | Base de données ou source non connectée ; dépendance indisponible |
| 504 | Délai d'attente de la requête dépassé |

Les violations de gouvernance sur `POST /data/sql` renvoient un code HTTP 403 avec un corps structuré : (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

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

Lorsque le proxy Zaychik Flight SQL est disponible (port 8480), les lots d'enregistrements sont diffusés de bout en bout sans matérialisation complète. (REQ-144) Bascule vers une matérialisation via la couche de requête fédérée si Zaychik est indisponible. (REQ-146)

---

## Endpoint gRPC Protobuf

Port `50051` (à remplacer par la variable d'environnement `GRPC_PORT` ou la configuration `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Transmettez le rôle dans la clé de métadonnées gRPC `x-provisa-role`. Si elle est absente, le serveur abandonne avec `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Téléchargez le proto spécifique au rôle depuis `GET /data/proto/{role_id}`. Seules les tables et colonnes visibles pour ce rôle apparaissent. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Chaque table produit un RPC de streaming `Query{TypeName}`. Les RPC `Insert{TypeName}` existent par symétrie de schéma mais abandonnent avec `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` est activé pour la découverte de services sans proto précompilé. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Le serveur gRPC ne démarre que lorsqu'un proto valide peut être compilé au démarrage. Si la construction du schéma échoue, le serveur gRPC ne démarre pas. (REQ-529)

---

## Pilote JDBC

Le pilote JDBC de Provisa (`provisa-jdbc-0.1.0.jar`) expose le catalogue sémantique aux outils de BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL de connexion :** `jdbc:provisa://host:port` (REQ-131)

Les domaines sont mappés sur des schémas JDBC. (REQ-127) Les tables utilisent leurs alias enregistrés. Les colonnes utilisent des alias et affichent les descriptions comme `REMARKS`. (REQ-128) Les méthodes de métadonnées standard (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exposent les relations sémantiques comme métadonnées de clé primaire/clé étrangère.

**Prise en charge SQL :** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Le pilote demande par défaut une redirection Arrow IPC. Les résultats sont diffusés lot par lot via `ArrowStreamReader`, limités à un lot d'enregistrements en mémoire. (REQ-293)

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

Les abonnements SSE sont disponibles sur `GET /data/subscribe/{table}`. (REQ-219, REQ-258) La livraison des notifications utilise un fournisseur enfichable sélectionné selon le type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY`, les sources MongoDB utilisent les Change Streams, et les sources Kafka utilisent des groupes de consommateurs. Le filtrage RLS et la validation de schéma s'appliquent quel que soit le fournisseur. Les sources WebSocket et RSS sont également prises en charge via le même endpoint. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]
