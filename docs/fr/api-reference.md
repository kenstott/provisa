# Référence de l'API

## Vue d'ensemble

Provisa expose des endpoints REST sous deux préfixes : `/data` pour l'exécution de requêtes et l'introspection de schéma, et `/admin` pour la gestion de la configuration. (REQ-043) La plupart des endpoints de données exigent un identifiant de rôle. Les opérations de configuration d'administration passent par une API GraphQL Strawberry à `/admin/graphql`. (REQ-164)

---

## Authentification

Lorsque `auth.provider` est configuré dans `provisa.yaml`, tous les endpoints sauf `/health` et `/setup/status` exigent un en-tête `Authorization: Bearer <token>`. (REQ-120) [tool-verified: `provisa/api/app.py`, `provisa/auth/wiring.py`]

Sans authentification configurée, le serveur tourne en mode développement. Toute requête est traitée comme l'identité `anonymous`, qui correspond à tous les rôles configurés avec un accès générique aux domaines. (REQ-535)

**La connexion (`POST /auth/login`)** est fournie par le fournisseur d'authentification actif lorsque `provider: basic` est configuré. (REQ-124) Le format des identifiants et la réponse dépendent du fournisseur.

**Introspection de l'identité :**

```http
GET /auth/me
```

Renvoie l'id, l'adresse e-mail, le nom d'affichage, les appartenances aux organisations et les affectations de rôles de l'utilisateur authentifié. En mode développement, renvoie `dev_mode: true` avec tous les identifiants de rôle listés. [tool-verified: `provisa/api/auth_router.py`]

```http
GET /auth/provider-type
```

Renvoie `{"provider": "<name>"}`, ou `{"provider": null}` lorsque l'authentification n'est pas configurée. [tool-verified: `provisa/api/auth_router.py`]

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

Le champ `role` ne sert qu'en mode développement (sans authentification). Lorsque l'authentification est active, le rôle de l'utilisateur authentifié est utilisé et le `role` du corps est ignoré.

Le champ `extensions` prend en charge le protocole APQ (Automatic Persisted Query) : (REQ-288)

```json
{
  "extensions": {"persistedQuery": {"sha256Hash": "<sha256-of-query>"}}
}
```

**En-têtes :**

- `X-Provisa-Role` — remplace le rôle (mode développement)
- `Accept` — format de réponse (voir Négociation de contenu)
- `Authorization` — `Bearer <token>` lorsque l'authentification est activée
- `X-Provisa-Redirect-Format` — type MIME de la sortie de redirection S3 (REQ-137)
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

**Réponse (multi-racine, en ligne et redirection mêlées) :**

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

Les requêtes multi-racines exécutent chaque champ racine indépendamment. Les champs sous le seuil de redirection reviennent en ligne ; ceux au-dessus sont redirigés. La clé `redirects` (au pluriel) associe les noms de champs à leurs informations de redirection. (REQ-029) [tool-verified: `provisa/api/data/endpoint.py`]

**En-têtes de cache :**

- `X-Provisa-Cache: HIT|MISS` (REQ-536)
- `X-Provisa-Cache-Age: <seconds>` (sur HIT) (REQ-536)

**Capacités requises :** `QUERY_DEVELOPMENT` pour toutes les requêtes, y compris l'introspection. [tool-verified: `provisa/api/data/endpoint.py:186-283`]

---

### Négociation de contenu

| En-tête Accept | Format |
| --- | --- |
| `application/json` | JSON (par défaut) |
| `application/x-ndjson` | JSON délimité par des sauts de ligne |
| `text/csv` | CSV |
| `application/vnd.apache.parquet` | Parquet |
| `application/vnd.apache.arrow.stream` | Arrow IPC |

(REQ-047, REQ-048, REQ-049, REQ-050) [tool-verified: `provisa/api/data/endpoint.py:84-90`]

---

### Redirection

Les résultats dépassant un seuil de lignes configuré (ou lorsque `X-Provisa-Redirect: true`) sont écrits sur S3 et une URL présignée est renvoyée. (REQ-029, REQ-044)

| Format de redirection | Écrit par | Mémoire |
| --- | --- | --- |
| `application/vnd.apache.parquet` | CTAS fédéré | Aucune — les données ne transitent jamais par Provisa |
| `application/x-orc` | CTAS fédéré | Aucune — les données ne transitent jamais par Provisa |
| `application/json` | Provisa | Limitée par la mémoire |
| `application/x-ndjson` | Provisa | Limitée par la mémoire |
| `text/csv` | Provisa | Limitée par la mémoire |
| `application/vnd.apache.arrow.stream` | Provisa | Limitée par la mémoire |

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

Les violations de gouvernance sur `POST /data/sql` renvoient un HTTP 403. (REQ-002, REQ-266)

**Réponse :** même format que `/data/graphql` (lignes JSON par défaut, négociées par `Accept`).

---

### `POST /data/query`

Endpoint de requête unifié. Accepte GraphQL, SQL ou Cypher — la syntaxe est détectée automatiquement. (REQ-267) [tool-verified: `provisa/api/data/endpoint_dev.py:509`]

Les requêtes Cypher peuvent aussi être soumises à l'endpoint dédié `POST /query/cypher`. (REQ-345)

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

L'endpoint enveloppe le SQL **gouverné** — l'instruction qui s'exécute réellement sous le rôle de l'appelant, après application de la sécurité au niveau des lignes et du masquage — dans la syntaxe EXPLAIN du dialecte. Le plan montre la version autorisée de la requête, pas l'entrée brute.

**Corps de la requête :**

```json
{
  "sql": "SELECT id, amount FROM orders",
  "role": "admin",
  "analyze": false
}
```

Passez `analyze: true` pour lancer EXPLAIN ANALYZE. La requête s'exécute et le plan porte des comptages de lignes et des temps réels. Tous les dialectes ne prennent pas en charge ANALYZE ; voir le tableau dans [Plans de requête et statistiques](engines.md#query-plans-and-statistics).

**Réponse :** `{"plan": "<plan text or JSON>", "dialect": "trino", "analyzed": false}`

`400` lorsque le dialecte ne prend pas en charge EXPLAIN, ou lorsque `analyze: true` est demandé sur un dialecte qui ne le permet pas (SQLite, par exemple). [tool-verified: `provisa/executor/explain.py:wrap_explain`, `analyze_sql`]

---

### `GET /data/engine/state`

Renvoie l'état courant du fragment de moteur sans le réveiller. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:892`]

L'interface interroge cet endpoint pour afficher une bannière de démarrage pendant le démarrage à froid du moteur. Il ne déclenche jamais de réveil — l'interrogation est sans risque et ne compte pas comme activité pour le collecteur d'inactivité.

**Réponse :**

```json
{"state": "ready"}
```

Valeurs possibles :

| État | Signification |
| --- | --- |
| `always-on` | Bureau, auto-hébergé, ou coordinateur fourni par le client — aucune gestion du cycle de vie |
| `ready` | Le fragment est actif et accepte les requêtes |
| `starting` | Démarrage à froid en cours |
| `stopped` | Le fragment est réduit à zéro |

[tool-verified: `provisa/federation/engine_wake.py:engine_state`]

---

### `POST /data/engine/prewarm`

Déclenche un réveil du moteur sans exécuter de requête. (REQ-1516) [tool-verified: `provisa/api/data/endpoint_dev.py:913`]

Renvoie immédiatement `202 Accepted`. Le réveil se déroule en arrière-plan. Utilisez-le si vous voulez que le moteur soit prêt avant l'arrivée de la première requête — depuis un planificateur qui lancera des requêtes quelques minutes plus tard, par exemple.

**Réponse :** `202 Accepted`, corps `{"started": true}`

[tool-verified: `provisa/federation/engine_wake.py:prewarm_engine`]

---

### `GET /data/rest/{domain_id}/{table_name}`

Endpoint REST simple, généré automatiquement pour chaque table enregistrée. La chaîne de requête est traduite en arguments GraphQL et la requête est compilée puis exécutée à travers le même pipeline (sécurité au niveau des lignes, masquage, routage) que GraphQL. (REQ-256) [tool-verified: `provisa/api/rest/generator.py:153`]

**Paramètres de requête :**

- `limit` — nombre maximal de lignes (≥ 1)
- `offset` — lignes à sauter (≥ 0)
- `fields` — noms de colonnes séparés par des virgules (par défaut, tous les champs scalaires)
- `filter` — tableau JSON d'objets de filtre `{"field", "comparator", "value"}`
- `orderBy` — tableau JSON d'objets de tri `{"field", "direction"}`

Le rôle authentifié est obligatoire ; les requêtes non authentifiées renvoient `401`. Une spécification OpenAPI de ces routes est servie à `GET /data/rest/openapi.json`, avec Swagger UI à `GET /data/rest/docs`.

#### Explorateur OpenAPI / Swagger UI

La page de l'explorateur OpenAPI (`/app/openapi`) intègre Swagger UI dans une iframe en bac à sable. La spécification est cadrée par rôle — seules les tables et colonnes visibles par le rôle courant y figurent — et éventuellement filtrée par domaine via le sélecteur de domaine. L'interface bascule automatiquement entre thème clair et thème sombre. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:20-34`]

La page charge le HTML de la spécification par `fetch()` plutôt que par un `src` d'iframe direct, si bien que la requête porte le jeton de session et que les requêtes relatives propres à Swagger UI se résolvent correctement sur la même origine. [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:44-69`]

Lorsqu'on y arrive depuis un lien « Ouvrir dans OpenAPI » du langage naturel, la page déplie automatiquement l'endpoint visé, remplit les paramètres de requête depuis l'URL générée (`aggregate`, `groupBy`, par exemple) et clique sur Exécuter — en interrogeant le DOM pour s'assurer que chaque étape est terminée avant de déclencher la suivante. (REQ-1359) [tool-verified: `provisa-ui/src/pages/OpenApiPage.tsx:94-171`]

---

### `GET /data/jsonapi/{domain_id}/{table_name}`

Endpoint conforme à [JSON:API](https://jsonapi.org), généré automatiquement pour chaque table enregistrée. Mêmes sécurité au niveau des lignes, masquage et routage que GraphQL. (REQ-257) [tool-verified: `provisa/api/jsonapi/generator.py:284`]

**En-tête `Accept` :** doit inclure `application/vnd.api+json` (le type de média JSON:API), sans quoi la requête renvoie `406`.

**Paramètres de requête :**

- `fields[<type>]` — jeux de champs partiels, par exemple `?fields[orders]=amount`
- `filter[<col>]` / `filter[<col>][<op>]` — par exemple `?filter[region]=US`, `?filter[amount][gt]=100`
- `sort` — séparés par des virgules, préfixe `-` pour l'ordre décroissant, par exemple `?sort=-created_at,amount`
- `page[number]` / `page[size]` — pagination
- `aggregate` — fonctions d'agrégation séparées par des virgules, exécutées au lieu de la récupération de lignes : `count`, `sum`, `avg`, `stddev`, `variance`, `min`, `max`. Utilisez `?aggregate=count,sum` pour n'en demander qu'une partie. Les réponses d'agrégation renvoient `data: null`, les résultats se trouvant dans `meta.aggregate`. (REQ-1359) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:238`]
- `groupBy` — noms de colonnes séparés par des virgules ; s'emploie avec `?aggregate=` pour regrouper les résultats. Seules les colonnes de l'énumération `DistinctOnColumn` de la table sont valides ; le serveur renvoie `400` pour toute colonne que le rôle ne peut pas voir. (REQ-1361) [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:447`]
- `includeNodes` — `true` pour inclure les colonnes scalaires de la table de base (et les scalaires des dimensions jointes nommées dans `include=`) dans le tableau `nodes` de chaque ligne de groupe. Obligatoire lorsqu'une requête de regroupement en langage naturel demande aussi le détail des dimensions. (REQ-1405)

Les réponses sont des objets ressource avec `type`/`id`/`attributes`. Les erreurs suivent la forme de l'objet erreur JSON:API.

#### Explorateur JSON:API

La page de l'explorateur JSON:API (`/app/jsonapi`) est une interface navigateur au-dessus de ces endpoints. Sélectionnez une table dans la liste groupée par domaine, puis configurez :

- **Champs** — choisissez les colonnes à inclure (jeu de champs partiel) ; ne cochez rien pour demander toutes les colonnes
- **Relations** — sélectionnez les noms de relations dérivées des clés étrangères à charger par `?include=`
- **Filtre** — champ, opérateur (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`) et valeur
- **Tri** — un champ, croissant ou décroissant
- **Agrégation** — choisissez les colonnes de regroupement dans la liste validée par le serveur, puis cochez une ou plusieurs fonctions d'agrégation ; lorsque des colonnes de regroupement sont sélectionnées, une case « Inclure les nœuds » ajoute les colonnes scalaires de la table de base à chaque ligne
- **Taille de page** — ressources par page, avec navigation premier/précédent/suivant/dernier

Les résultats s'affichent dans une vue de synthèse mise en forme (cartes de ressources avec ancres de relations cliquables) ou dans un onglet JSON brut. L'URL de requête en vigueur est affichée et peut être copiée. La table sélectionnée et la taille de page persistent d'une session à l'autre dans `localStorage`. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx`]

Lorsqu'on y arrive depuis un lien « Ouvrir dans JSON:API » du langage naturel, l'explorateur présélectionne la table et amorce le sélecteur d'agrégation depuis les paramètres de la requête générée, puis lance la requête automatiquement. [tool-verified: `provisa-ui/src/pages/JsonApiPage.tsx:460-479`]

---

### `POST /query/nl`

Soumet une question en langage naturel. Le service démarre une tâche asynchrone et renvoie immédiatement `202 Accepted` avec un `job_id`. Exige un fournisseur LLM configuré dans la section `ai_models` de la configuration. (REQ-354) [tool-verified: `provisa/api/rest/nl_router.py:50`]

**Corps de la requête :**

```json
{"q": "How many orders were placed last month?", "role": "admin"}
```

Renvoie `{"job_id": "<id>"}`. Dépasser la limite de débit du langage naturel propre au rôle renvoie `429` avec un en-tête `Retry-After`. (REQ-370)

**Récupérer le résultat :**

- `GET /query/nl/{job_id}` — interrogation. Renvoie le document de la tâche.
- `GET /query/nl/{job_id}/stream` — SSE. Un événement `branch` par cible de génération à mesure qu'elle se termine, puis un événement `done`. (REQ-357, REQ-358)

Trois boucles de génération (Cypher, GraphQL, SQL) s'exécutent en parallèle, chacune validée par le compilateur et affinée en cas d'erreur. (REQ-355) L'invite est cadrée sur le schéma visible par le rôle. (REQ-356) Le document de résultat indexe chaque branche par cible : (REQ-357) [tool-verified: `provisa/nl/job.py:69`]

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

Une branche qui épuise sa limite d'itérations renvoie `query: null`, `result: null` et une chaîne `error`. Chaque requête générée s'exécute avec les droits du consommateur et la gouvernance de l'étape 2 appliquée — le service ne contourne jamais la gouvernance. (REQ-359)

#### Regroupement en langage naturel avec détail des dimensions (REQ-1405)

Lorsqu'une requête de regroupement en langage naturel projette aussi des colonnes d'une table de dimension jointe — « nombre de demandes par utilisateur, avec le nom et l'e-mail de l'utilisateur », par exemple — l'exécuteur dérive des chemins pointés par champ (`dim_paths`) à partir des colonnes de dimension projetées dans le SELECT. Ces chemins alimentent le paramètre `includeNodes=` des URL générées par les panneaux JSON:API et OpenAPI, de sorte que ces panneaux demandent les mêmes champs de dimension jointe que ceux résolus par les branches SQL et GraphQL. Sans cela, `includeNodes=true` ne renverrait que les champs scalaires de la table d'agrégation de base. (REQ-1405) [tool-verified: `docs/arch/requirements.md:REQ-1405`]

Sur le panneau gRPC, le `{Type}GroupByRequest` généré porte `include_nodes` (booléen) et `include` (chaîne répétée de noms de champs de relation). Le `{Type}GroupByRow` renvoyé comprend un champ `nodes` typé contenant les lignes de détail des dimensions. [tool-verified: `provisa/grpc/query_ir.py:168-196`]

---

### `GET /data/sdl`

Renvoie le SDL GraphQL du schéma d'un rôle. (REQ-008) [tool-verified: `provisa/api/data/sdl.py:137`]

**En-têtes :** `X-Role: <role_id>` (obligatoire)

**Paramètres de requête :**

- `domain` — identifiants de domaine séparés par des virgules. Lorsqu'il est renseigné, la réponse est filtrée sur le ou les domaines nommés et sur les tables accessibles depuis eux.

**Réponse :** SDL GraphQL en `text/plain`.

---

### `GET /data/introspection`

Renvoie le JSON d'introspection GraphQL, éventuellement filtré par domaine. [tool-verified: `provisa/api/data/sdl.py:200`]

**En-têtes :** `X-Provisa-Role: <role_id>` (obligatoire)

**Paramètres de requête :** `domain` — identifiants de domaine séparés par des virgules.

**Réponse :** résultat d'introspection en `application/json`.

---

### `GET /data/graph-schema`

Renvoie la vue graphe du schéma du rôle : les libellés de nœuds et leurs types de relations, à destination des clients Cypher et graphe. Inclut `pk_columns` par libellé de nœud pour que les appelants puissent déterminer les colonnes de clé primaire. (REQ-398) [tool-verified: `provisa/api/rest/cypher_router.py:689`]

**Réponse :** `application/json` avec `node_labels` (portant chacun `pk`/`pk_columns`) et `relationship_types`.

Chaque type de relation porte aussi `junction_table_name` et `properties` (REQ-1586). Sur une arête adossée à une jonction, le premier nomme la table associative qu'elle traverse et le second liste les colonnes de cette table lisibles comme `r.attr` et filtrables dans `WHERE` ; sur une arête adossée à une clé étrangère, le nom vaut `null` et la liste de propriétés est vide, ce qui permet à un client de distinguer les deux. La table de jonction elle-même n'est jamais un label de nœud — elle est l'arête, elle n'a donc ni pastille dans un client graphe ni ligne dans `node_labels`. [tool-verified: `provisa/api/rest/cypher_router.py:797-805`, `provisa/cypher/label_map.py:378-397`]

---

### `GET /data/domains`

Renvoie les identifiants de domaine accessibles au rôle demandeur. [tool-verified: `provisa/api/data/sdl.py:116`]

**En-têtes :** `X-Role: <role_id>` (obligatoire)

**Réponse :** `["sales", "support", ...]`

---

### `GET /data/schema-version`

Renvoie la chaîne de version courante du schéma. Elle combine un nonce propre au démarrage et un compteur de reconstructions. Les clients s'en servent pour invalider leurs caches de schéma après un redémarrage du serveur. (REQ-537) [tool-verified: `provisa/api/data/sdl.py:102`]

**Réponse :** `{"version": "<boot-id>-<counter>"}`

---

### `GET /data/proto/{role_id}`

Renvoie le fichier `.proto` généré automatiquement pour un rôle. [tool-verified: `provisa/api/data/endpoint_dev.py:49`]

**Réponse :** schéma protobuf en `text/plain`.

Chaque table enregistrée produit un `message` proto. Les relations produisent des champs de message imbriqués. Correspondance des types : `integer → int32`, `bigint → int64`, `varchar → string`, `decimal → double`, `boolean → bool`, `timestamp → google.protobuf.Timestamp`. (REQ-538)

---

### `GET /data/subscribe/{table}`

Flux Server-Sent Events pour les notifications de changement en temps réel d'une table. (REQ-219, REQ-258) [tool-verified: `provisa/api/data/subscribe.py:239`]

La distribution des notifications s'appuie sur un fournisseur enfichable choisi selon le type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY` (via asyncpg), les sources MongoDB utilisent les Change Streams (`collection.watch()`) et les sources Kafka utilisent des groupes de consommateurs. Chaque fournisseur implémente une interface de surveillance asynchrone commune. Le filtrage par sécurité au niveau des lignes et la validation de schéma s'appliquent quel que soit le fournisseur. (REQ-258) Les sources WebSocket et RSS sont également prises en charge. (REQ-338, REQ-342)

**En-tête — `X-Provisa-Sink` :** renseignez une cible Kafka (par exemple `kafka://broker:9092/topic`) pour rediriger les événements de changement vers un récepteur Kafka au lieu de la réponse SSE. Le serveur lance un consommateur récepteur et renvoie `202 Accepted` plutôt qu'un flux ouvert. (REQ-812) [tool-verified: `provisa/api/data/subscription_sse.py:137`]

---

## Endpoints REST d'administration

### Configuration

#### `GET /admin/config`

Télécharge le `provisa.yaml` courant en `application/x-yaml` avec un en-tête `Content-Disposition: attachment`. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:19`]

#### `PUT /admin/config`

Téléverse un YAML de configuration révisé. Le serveur écrit une sauvegarde `.bak`, enregistre le nouveau fichier et recharge tous les schémas, sources et vues matérialisées. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:32`]

**Corps de la requête :** contenu YAML brut.

**Réponse :**

```json
{"success": true, "message": "Config uploaded and reloaded"}
```

En cas d'échec du rechargement : `{"success": false, "message": "<error>"}`.

#### `GET /admin/config/live`

Télécharge la **configuration active courante** — la configuration telle que Provisa l'écrirait aujourd'hui, reflétant chaque table, relation, domaine, rôle et règle de sécurité au niveau des lignes créés par l'administration depuis le démarrage. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:67`]

Le fichier sur disque peut être en retard sur l'état actif si des changements ont été faits par l'API d'administration sans téléversement ultérieur. Cet endpoint comble cet écart : sa sortie est exactement ce que `PUT /admin/config` devrait recevoir pour que le fichier sur disque corresponde à l'état actif.

Renvoie `application/x-yaml` avec `Content-Disposition: attachment; filename=provisa.live.yaml`.

#### `GET /admin/config/diff`

Renvoie les deux côtés de la comparaison de configuration — `original` (la référence de démarrage) et `current` (l'état actif) — normalisés à l'identique, de sorte que la comparaison ne fasse ressortir que les vrais changements, ni les réordonnancements ni la dérive des commentaires. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:82`]

**Réponse :**

```json
{"original": "<yaml>", "current": "<yaml>"}
```

#### `POST /admin/config/patch`

Génère un correctif au format diff unifié entre la référence et la configuration envoyée. (REQ-164) [tool-verified: `provisa/api/admin/settings_router.py:93`]

Envoyez le YAML révisé dans le corps de la requête. La réponse est un fichier `text/x-patch` (`provisa.config.patch`) que `git apply` ou `patch` consomme directement — utile pour valider dans un pipeline CI/CD des changements de configuration faits depuis l'interface.

---

### Paramètres

#### `GET /admin/settings`

Renvoie les paramètres courants de la plateforme en JSON. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:50`]

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

Met à jour les paramètres de la plateforme à chaud. Tous les champs sont facultatifs — seules les clés présentes dans le corps sont mises à jour. (REQ-165) [tool-verified: `provisa/api/admin/settings_router.py:100`]

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

Champs modifiables par section :

- `redirect` : `enabled`, `threshold`, `default_format`, `ttl`
- `sampling` : `default_sample_size`
- `cache` : `default_ttl`
- `naming` : `domain_prefix`, `convention` — écrit dans le fichier de configuration et déclenche un rechargement du schéma (REQ-253)
- `relationships` : `auto_track_fk` — régit uniquement le suivi des clés étrangères. Une relation adossée à une jonction est déclarée lors de l'enregistrement de la table et n'est jamais inférée, ce réglage ne l'atteint donc pas. (REQ-1586)
- `otel` : `endpoint`, `service_name`, `sample_rate`, `support_endpoint`, `support_redact_sql_literals`, `support_redact_attributes`

**Réponse :**

```json
{"success": true, "updated": ["otel.support_endpoint", "cache.default_ttl"]}
```

---

### Modèles d'IA

#### `GET /admin/ai-models`

Renvoie les affectations de modèles d'IA de l'organisation agissante, le registre des modèles vectoriels et la limite de débit du langage naturel. (REQ-464, REQ-1349) [tool-verified: `provisa/api/admin/ai_models_router.py:58`]

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

Les clés d'API ne sont jamais renvoyées — `api_keys_set` indique seulement si chaque fournisseur dispose d'une clé configurée. Les changements prennent effet à la requête suivante ; aucun redémarrage n'est nécessaire. (REQ-1349)

#### `PUT /admin/ai-models`

Met à jour les affectations de modèles d'IA de l'organisation, le registre des modèles vectoriels ou la limite de débit du langage naturel. Prend effet à la requête suivante. [tool-verified: `provisa/api/admin/ai_models_router.py:148`]

#### `GET /admin/ai-models/vendors/{vendor}/models`

Renvoie les noms de modèles qu'un fournisseur sert actuellement, pour le sélecteur de modèle. (REQ-1395, REQ-1398, REQ-1409) [tool-verified: `provisa/api/admin/ai_models_router.py:89`]

La liste est lue en direct depuis l'API de listage de modèles du fournisseur, avec la clé configurée par l'organisation — ou l'identifiant du déploiement lorsque l'organisation n'en a pas. Un modèle sorti après la publication de cette version est sélectionnable le jour même où le fournisseur le sert.

Renvoie `400` lorsque le fournisseur ne publie pas d'API de listage de modèles (saisissez alors le nom du modèle directement) ou lorsqu'aucune clé n'est disponible. [tool-verified: `provisa/api/admin/ai_models_router.py:109-128`]

---

### Moteur de fédération

#### `GET /admin/federation-engine`

Renvoie la sélection courante du moteur de fédération, sa configuration de connexion et le registre complet des moteurs sélectionnables. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:730`]

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

La clé `current` désigne le moteur qui tourne en ce moment ; `persisted` est ce qui est écrit dans le fichier de configuration et se chargera au prochain redémarrage. Les deux divergent lorsque la configuration a changé mais que le service n'a pas encore redémarré.

#### `PUT /admin/federation-engine`

Persiste une sélection de moteur de fédération. (REQ-916) [tool-verified: `provisa/api/admin/settings_router.py:774`]

**Corps de la requête :**

```json
{"engine": "trino", "federation_engine_url": "http://trino-coordinator:8080"}
```

La sélection est écrite dans la configuration de la plateforme. Elle prend effet au redémarrage suivant du service — le moteur est choisi une seule fois, au démarrage.

---

### Politique de domaines

#### `POST /admin/domain-policy`

Modifie la politique de domaines de l'organisation agissante (`use_domains` / `default_domain`). (REQ-165, REQ-1266, REQ-1349) [tool-verified: `provisa/api/admin/settings_router.py:632`]

C'est une opération destructive, cadrée sur l'organisation agissante. Chaque source, table, domaine et relation enregistrés est purgé puis reconstruit sous la nouvelle politique. Utilisez-la pour faire passer une organisation d'un espace de noms par domaines à un espace plat (ou l'inverse).

**Corps de la requête :**

```json
{
  "use_domains": true,
  "default_domain": "default"
}
```

`use_domains: null` efface le réglage propre à l'organisation et revient au réglage du déploiement. `use_domains: false` exige `default_domain` (le nom du domaine unique où atterrissent toutes les tables). La reconstruction du catalogue est synchrone ; la réponse revient une fois les schémas prêts.

---

### Observabilité

#### `GET /admin/traces/recent`

Renvoie jusqu'à N spans récemment terminés depuis le tampon de spans en mémoire. (REQ-302) [tool-verified: `provisa/api/admin/settings_router.py:317`]

**Paramètres de requête :** `limit` (50 par défaut, 200 au maximum)

**Réponse :** `{"traces": [...]}`

#### `POST /admin/query-engine/reload-catalog`

Recharge à chaud un catalogue nommé dans le coordinateur du moteur de fédération via son API REST. Reconnecte la connexion interne de Provisa et rejoue le DDL OTel. [tool-verified: `provisa/api/admin/settings_router.py:208`]

**Paramètres de requête :** `catalog` (`"otel"` par défaut)

**Réponse :**

```json
{"success": true, "errors": []}
```

#### `POST /admin/query-engine/restart`

Redémarre le conteneur du moteur de fédération (mono-nœud, développement uniquement). [tool-verified: `provisa/api/admin/settings_router.py:287`]

**Paramètres de requête :** `container` (par défaut la variable d'environnement `QUERY_ENGINE_CONTAINER`, puis `"trino"`)

---

### Découverte

#### `POST /admin/discover/relationships`

Déclenche la découverte de relations. Lance systématiquement l'introspection des clés étrangères depuis le moteur de fédération. (REQ-018) Lance l'inférence par LLM si `ANTHROPIC_API_KEY` est renseignée. (REQ-167) [tool-verified: `provisa/api/admin/discovery.py:55`]

**Corps de la requête :**

```json
{
  "scope": "domain",
  "domain_id": "sales"
}
```

`scope` doit valoir `"table"`, `"domain"` ou `"cross-domain"`. Pour la portée `"table"`, `table_id` (entier) est obligatoire. Pour la portée `"domain"`, `domain_id` est obligatoire.

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

### Exploration de source

#### `POST /admin/sources/crawl`

Explore une source de données pour introspecter son schéma et enregistrer ses tables. (REQ-012) [tool-verified: `provisa/api/admin/crawl_router.py:36`]

---

### Recherche de tables dans une source

#### `GET /admin/sources/{source_id}/tables/search`

Recherche par nom les tables disponibles (pas encore enregistrées) dans une source. [tool-verified: `provisa/api/admin/table_search_router.py:103`]

---

### Profilage de table

#### `POST /admin/tables/{table_id}/profile`

Établit un profil de colonnes sur une table enregistrée — cardinalité, min/max, taux de valeurs nulles. [tool-verified: `provisa/api/admin/table_profile_router.py:28`]

---

### Descriptions de source

#### `POST /admin/source-meta/db-description`

Génère, avec l'aide d'un LLM, des descriptions pour les tables et colonnes d'une source. [tool-verified: `provisa/api/admin/source_meta_router.py:48`]

---

### Stockage objet (REQ-1046, REQ-1048, REQ-1049)

#### `GET /admin/org-storage`

Indique l'empreinte de stockage de l'organisation agissante face à son allocation de plateforme, et si l'organisation a enregistré son propre magasin. [tool-verified: `provisa/api/admin/org_storage_router.py:69`]

Lorsque l'organisation a enregistré son propre DSN, ses matérialisations y sont écrites et ne sont plus comptées dans l'allocation. Le DSN lui-même n'est jamais renvoyé.

#### `PUT /admin/org-storage`

Enregistre (ou efface) le magasin de matérialisation propre à l'organisation. [tool-verified: `provisa/api/admin/org_storage_router.py:81`]

**Corps de la requête :**

```json
{"storage_url": "s3://my-bucket/provisa?region=us-east-1&access_key=..."}
```

Le DSN est validé auprès du moteur de fédération avant d'être accepté — un DSN inutilisable échoue à l'enregistrement, pas des heures plus tard lors d'un rafraîchissement. La valeur est chiffrée au repos et n'est jamais renvoyée par GET.

Envoyez `storage_url: null` pour effacer le magasin propre à l'organisation et ramener ses matérialisations vers le magasin de la plateforme (et son allocation). Le runtime de l'organisation est reconstruit dans le même appel : le nouveau magasin est donc effectif immédiatement. [tool-verified: `provisa/api/admin/org_storage_router.py:123-138`]

---

### Chiffrement de l'organisation (REQ-1574)

#### `GET /admin/org-encryption`

Renvoie l'état courant de la clé de l'organisation : empreinte, id et provenance. Ne renvoie jamais de matériel de clé. [tool-verified: `provisa/api/admin/org_encryption_router.py:53`]

Lorsque l'organisation n'a défini aucune clé, renvoie `{"configured": false}`. Toute organisation démarre dans cet état et hérite de la clé du déploiement.

#### `PUT /admin/org-encryption`

Définit ou fait tourner la clé de chiffrement au repos de l'organisation. [tool-verified: `provisa/api/admin/org_encryption_router.py:68`]

**Corps de la requête :**

```json
{"key_b64": "<32 raw bytes, base64-encoded>"}
```

Omettez `key_b64` pour que Provisa génère une clé — la voie la plus sûre, puisque la clé n'apparaît alors ni dans un presse-papiers ni dans un journal de requêtes. Fournir `key_b64` revient à apporter votre propre clé.

La rotation ajoute une nouvelle entrée active au trousseau et conserve l'ancienne, si bien que les données écrites sous la clé précédente restent lisibles. Une rotation n'est pas un rechiffrement. Il n'existe aucun endpoint de suppression : retirer la dernière clé rendrait illisible chaque charge utile enveloppée. [tool-verified: `provisa/api/admin/org_encryption_router.py:75`]

Le trousseau actif est réassocié dans le même appel : la prochaine écriture chiffrée emploie donc la nouvelle clé immédiatement.

---

### Import Hasura / DDN (REQ-1483)

#### `POST /admin/import/hasura/preview`

Convertit une archive de projet Hasura v2 ou DDN en configuration Provisa proposée, sans rien écrire. [tool-verified: `provisa/api/admin/import_router.py`]

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

`flavor` vaut `"auto"` (détecté d'après la structure de l'archive), `"hasura_v2"` ou `"ddn"`.

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

Rien n'est persisté. L'aperçu n'est pas mis en cache côté serveur ; `apply` prend le YAML que vous fournissez, si bien que ce qui s'applique est exactement ce qui a été relu (et éventuellement modifié).

#### `POST /admin/import/hasura/apply`

Charge dans l'organisation agissante une configuration précédemment prévisualisée. [tool-verified: `provisa/api/admin/import_router.py`]

**Corps de la requête :**

```json
{"config_yaml": "<yaml string>"}
```

Emprunte le même chemin de rechargement à chaud que `PUT /admin/config`. Le catalogue, les schémas et les pools de l'organisation sont reconstruits avant le retour de la réponse.

---

### Échange Apache Ossie (REQ-1316, REQ-1321)

#### `GET /admin/ossie`

Exporte le modèle gouverné de l'organisation sous forme de document YAML Apache Ossie (en incubation). (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py`]

Le document est dérivé de l'état actif à chaque requête — jamais mis en cache — donc il ne peut pas être périmé. Les tables deviennent des objets `dataset`, les colonnes des objets `field`, et les relations correspondent aux objets `relationship` d'Ossie.

Renvoie `text/yaml` avec `Content-Disposition: attachment; filename=provisa-ossie.yaml`.

#### `POST /admin/ossie/import`

Analyse un document Ossie YAML ou JSON et renvoie des propositions d'enregistrement de tables et de relations. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py`]

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

Rien n'est enregistré. Passez par l'écran de revue de l'interface d'administration pour accepter ou élaguer les propositions avant qu'une mutation ne parte.

---

### Actions (fonctions et webhooks)

Tous les endpoints se trouvent sous le préfixe `/admin/actions`. (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:24`]

Chaque invocation — depuis GraphQL, SQL, Cypher, Bolt, Arrow Flight, le `run_sql` de MCP et le gRPC Provisa — passe par un exécuteur gouverné unique qui applique `writable_by` et la gouvernance de façon uniforme. (REQ-1156) [tool-verified: `provisa/api/data/action_exec.py`] Voir [docs/integrations.md](integrations.md#invoking-commands-across-protocols) pour la syntaxe d'appel propre à chaque protocole.

#### `GET /admin/actions`

Renvoie toutes les fonctions de base de données et tous les webhooks suivis. (REQ-242) [tool-verified: `provisa/api/admin/actions_router.py:104`]

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

Chaque objet webhook porte un booléen `approved`. Un webhook est approuvé dès qu'un intendant exécute sa demande de création (REQ-209) ; les webhooks déclarés dans la configuration sont approuvés d'office. Un webhook non approuvé est enregistré mais exposé sur aucune surface. [tool-verified: `provisa/api/admin/actions_router.py:124-131`]

#### `POST /admin/actions/functions`

Enregistre une fonction suivie (commande). (REQ-205) [tool-verified: `provisa/api/admin/actions_router.py:117`]

**Champs principaux :**

| Champ | Obligatoire | Description |
| --- | --- | --- |
| `name` | Oui | Nom unique de la commande |
| `kind` | Oui | `"query"` → champ Query GraphQL ; `"mutation"` → champ Mutation |
| `implKind` | Non | Mode d'exécution de la commande — voir le tableau ci-dessous (`source_procedure` par défaut) |
| `binding` | Non | Détails de connexion propres à `implKind` (objet JSON) |
| `returnSchema` | Non | JSON Schema `{type:"array", items:{type:"object", properties:{...}}}` — rend la commande productrice d'ensembles sur toutes les surfaces |
| `arguments` | Non | Définitions d'arguments `[{name, type}]` ; l'ordre positionnel compte pour les appelants SQL et Bolt |
| `visibleTo` | Non | Identifiants de rôle autorisés à appeler la commande |
| `writableBy` | Non | Identifiants de rôle autorisés à l'invoquer comme mutation |
| `domainId` | Non | Domaine de placement GraphQL et de contrôle d'accès |

**Valeurs de `implKind` :**

| `implKind` | Ce qui s'exécute | Champs de `binding` |
| --- | --- | --- |
| `source_procedure` | Procédure stockée sur une source enregistrée (par défaut) | `sourceId`, `schemaName`, `functionName` |
| `script` | Script côté serveur | `script` |
| `http` | Appel HTTP sortant | `url`, `method` |
| `grpc` | Appel gRPC sortant vers un serveur externe | `target`, `method` |
| `python` | Appelable Python hébergé par Provisa (REQ-885) | `callable` (par exemple `"demo.py_functions:random_dataset"`) |

Les commandes de démonstration `random_python_set` (`implKind: python`) et `random_grpc_set` (`implKind: grpc`) montrent en pratique des commandes productrices d'ensembles avec `returnSchema` ; toutes deux se trouvent dans `config/provisa-install.yaml`. [tool-verified: `config/provisa-install.yaml:809-856`]

#### `PUT /admin/actions/functions/{name}`

Met à jour une fonction suivie par son nom. [tool-verified: `provisa/api/admin/actions_router.py:182`]

#### `DELETE /admin/actions/functions/{name}`

Supprime une fonction suivie par son nom. [tool-verified: `provisa/api/admin/actions_router.py:233`]

#### `POST /admin/actions/webhooks`

Enregistre un webhook suivi. (REQ-209) Enregistrer ou mettre à jour un webhook met en file une demande d'approbation d'intendant — le webhook ne devient actif sur toutes les surfaces qu'après approbation. Les webhooks déclarés dans la configuration sont approuvés d'office. **Champs du corps de la requête :** `name`, `url`, `method`, `timeoutMs`, `returns`, `inlineReturnType`, `arguments`, `visibleTo`, `domainId`, `description`, `kind`. [tool-verified: `provisa/api/admin/actions_router.py:132`, `provisa/api/admin/actions_router.py:325-331`]

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
| `GET` | `/admin/roles/` | Lister tous les rôles |
| `POST` | `/admin/roles/` | Créer un rôle |
| `PUT` | `/admin/roles/{role_id}` | Mettre à jour un rôle |
| `DELETE` | `/admin/roles/{role_id}` | Supprimer un rôle |

[tool-verified: `provisa/api/admin/roles_router.py`]

---

### Utilisateurs

Tous les endpoints se trouvent sous le préfixe `/admin/users`. [tool-verified: `provisa/api/admin/local_users_router.py:21`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `POST` | `/admin/users/` | Créer un utilisateur local |
| `GET` | `/admin/users/` | Lister les utilisateurs locaux |
| `GET` | `/admin/users/{user_id}` | Obtenir un utilisateur |
| `PUT` | `/admin/users/{user_id}` | Mettre à jour un utilisateur |
| `PATCH` | `/admin/users/{user_id}/password` | Changer le mot de passe |
| `DELETE` | `/admin/users/{user_id}` | Supprimer un utilisateur |
| `GET` | `/admin/users/{user_id}/assignments` | Lister les affectations de rôles |
| `POST` | `/admin/users/{user_id}/assignments` | Ajouter une affectation de rôle |
| `DELETE` | `/admin/users/{user_id}/assignments/{assignment_id}` | Retirer une affectation de rôle |

---

### Organisations

Tous les endpoints se trouvent sous `/admin/orgs`. [tool-verified: `provisa/api/admin/orgs_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/orgs/` | Lister les organisations |
| `POST` | `/admin/orgs/` | Créer une organisation |
| `PUT` | `/admin/orgs/{org_id}` | Mettre à jour une organisation |
| `DELETE` | `/admin/orgs/{org_id}` | Supprimer une organisation |
| `GET` | `/admin/orgs/{org_id}/members` | Lister les membres |
| `POST` | `/admin/orgs/{org_id}/members` | Ajouter un membre |
| `DELETE` | `/admin/orgs/{org_id}/members/{user_id}` | Retirer un membre |

---

### Invitations

Tous les endpoints se trouvent sous `/admin/invites`. [tool-verified: `provisa/api/admin/invites_router.py:18`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `POST` | `/admin/invites/` | Créer une invitation |
| `GET` | `/admin/invites/` | Lister les invitations en attente |
| `DELETE` | `/admin/invites/{token}` | Révoquer une invitation |

---

### GraphQL d'administration

#### `POST /admin/graphql`

Endpoint GraphQL Strawberry pour toutes les opérations d'administration : CRUD des sources et des tables, gestion des relations, configuration des domaines, règles de sécurité au niveau des lignes, contrôle du cache, conventions de nommage, gestion des tâches planifiées et compilation de requêtes. (REQ-164) [tool-verified: `provisa/api/app.py:2171`]

**Mutations principales :**

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

### Installation

#### `GET /setup/status`

Renvoie l'état de l'installation initiale. Toujours non authentifié. (REQ-539) [tool-verified: `provisa/api/setup_router.py:100`]

#### `POST /setup/`

Termine l'installation initiale. [tool-verified: `provisa/api/setup_router.py:142`]

---

## Contrôle de santé

#### `GET /health` ou `HEAD /health`

Renvoie `{"status": "ok"}`. Toujours non authentifié. (REQ-539) [tool-verified: `provisa/api/app.py:2258`]

---

## Réponses d'erreur

| Statut | Signification |
| --- | --- |
| 400 | Requête invalide, erreur de validation ou erreur d'analyse SQL |
| 401 | Jeton d'authentification absent ou invalide |
| 403 | Capacités insuffisantes ; violation de gouvernance |
| 404 | Rôle, ressource ou fichier de configuration introuvable |
| 422 | En-tête obligatoire manquant (par exemple `X-Role`) |
| 503 | Base de données ou source non connectée ; dépendance indisponible |
| 504 | Délai de la requête dépassé |

Les violations de gouvernance sur `POST /data/sql` renvoient un HTTP 403 avec un corps structuré : (REQ-002) [tool-verified: `provisa/api/data/endpoint_dev.py:184-190`]

```json
{
  "detail": {
    "violations": [
      {"code": "V000", "message": "Table 'orders' is not accessible for role 'analyst'"}
    ]
  }
}
```

Toutes les autres erreurs emploient : `{"detail": "<message>"}`.

---

## Endpoint Arrow Flight

Port `8815`. Transport colonnaire Arrow natif sur gRPC. (REQ-143, REQ-045) [tool-verified: `provisa/api/flight/server.py`]

Les requêtes et la découverte du catalogue sont toutes deux disponibles sur la même connexion. Le pipeline de gouvernance complet (sécurité au niveau des lignes, masquage, échantillonnage) s'applique à chaque requête. (REQ-130, REQ-143)

**Format du ticket** (JSON) :

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

Lorsque le proxy Zaychik Flight SQL est disponible (port 8480), les lots d'enregistrements circulent de bout en bout sans matérialisation complète. (REQ-144) À défaut, la matérialisation passe par la couche de requête fédérée si Zaychik est indisponible. (REQ-146)

---

## Endpoint gRPC Protobuf

Port `50051` (modifiable par la variable d'environnement `GRPC_PORT` ou la clé de configuration `server.grpc_port`). (REQ-529) [tool-verified: `provisa/grpc/server.py`, `provisa/api/app.py`]

Passez le rôle dans la clé de métadonnées gRPC `x-provisa-role`. En son absence, le serveur interrompt avec `UNAUTHENTICATED`. [tool-verified: `provisa/grpc/server.py`]

Téléchargez le proto propre au rôle depuis `GET /data/proto/{role_id}`. Seules les tables et colonnes visibles par ce rôle y figurent. (REQ-039)

```proto
service ProvisaService {
  rpc QueryOrders (QueryOrdersRequest) returns (stream Orders);
  rpc InsertOrders (InsertOrdersRequest) returns (InsertOrdersResponse);
}
```

Chaque table produit un RPC de flux `Query{TypeName}`. Des RPC `Insert{TypeName}` existent par symétrie de schéma mais interrompent avec `UNIMPLEMENTED`. [tool-verified: `provisa/grpc/server.py`]

`grpc_reflection.v1alpha` est activé pour permettre la découverte de services sans proto précompilé. (REQ-529) [tool-verified: `provisa/grpc/reflection.py`]

```bash
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -H 'x-provisa-role: analyst' \
  -d '{}' localhost:50051 ProvisaService/QueryOrders
```

Le serveur gRPC ne démarre que si un proto valide peut être compilé au démarrage. Si la construction du schéma échoue, le serveur gRPC ne démarre pas. (REQ-529)

#### RPC d'agrégation et de regroupement (REQ-1359, REQ-1361, REQ-1405)

Lorsqu'une table porte `enable_aggregates`, le proto généré comprend deux RPC supplémentaires à côté de `Query{TypeName}` :

- **`Query{TypeName}Aggregate`** — renvoie les scalaires d'agrégation de la table (`count` ; `sum`, `avg`, `stddev`, `variance` par colonne numérique ; `min`, `max` par colonne comparable)
- **`Query{TypeName}GroupBy`** — renvoie une ligne par clé de groupe, avec des sous-champs d'agrégation et, éventuellement, les scalaires de la table de base et les lignes des dimensions jointes dans un champ `nodes`

Les deux passent par le même pipeline d'agrégation du compilateur que les champs racines `{field}_aggregate` et `{field}_group_by` de GraphQL — pas d'implémentation d'agrégation distincte. (REQ-1359) [tool-verified: `provisa/grpc/query_ir.py:133-196`]

**Champ `funcs` (REQ-1361).** Le message de requête accepte un champ `funcs` de chaînes répétées. Les valeurs valides sont `count`, `sum`, `avg`, `stddev`, `variance`, `min` et `max`. Lorsque `funcs` est omis, chaque fonction que le schéma expose pour cette table est demandée. Lorsqu'il est renseigné, seules les fonctions nommées apparaissent. Si aucune des fonctions nommées ne s'applique aux types de colonnes de la table, la requête retombe sur `count`. [tool-verified: `provisa/grpc/query_ir.py:66`, `provisa/grpc/query_ir.py:75-97`]

**Champs `include_nodes` et `include` (REQ-1405).** Les requêtes `Query{TypeName}GroupBy` peuvent poser `include_nodes: true` pour inclure les colonnes scalaires de la table de base dans le champ `nodes` de chaque ligne. Le champ `include` de chaînes répétées nomme les champs de relation plusieurs-à-un dont les colonnes scalaires sont elles aussi imbriquées dans `nodes`. Cela correspond au comportement `?includeNodes=` / `?include=` de JSON:API. [tool-verified: `provisa/grpc/query_ir.py:168-195`]

---

## Pilote JDBC

Le pilote JDBC Provisa (`provisa-jdbc-0.1.0.jar`) expose le catalogue sémantique aux outils de BI (Tableau, PowerBI, DBeaver). (REQ-126)

**URL de connexion :** `jdbc:provisa://host:port` (REQ-131)

Les domaines correspondent aux schémas JDBC. (REQ-127) Les tables emploient leurs alias enregistrés. Les colonnes emploient leurs alias et exposent leurs descriptions en `REMARKS`. (REQ-128) Les méthodes de métadonnées standard (`getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`) exposent les relations sémantiques comme métadonnées de clés primaires et étrangères.

**Prise en charge SQL :** `SELECT * FROM <alias> [WHERE col = 'value']`. (REQ-129)

Le pilote demande par défaut une redirection Arrow IPC. Les résultats circulent lot par lot via `ArrowStreamReader`, avec au plus un lot d'enregistrements en mémoire. (REQ-293)

---

## Format de l'argument `orderBy`

L'argument `order_by` emploie des objets `{column: direction}` avec une énumération de direction à six valeurs : (REQ-200)

```json
{
  "query": "{ orders(order_by: [{created_at: desc_nulls_last}]) { id created_at } }",
  "role": "admin"
}
```

Directions prises en charge : `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. (REQ-201)

---

## Abonnements

Les abonnements SSE sont disponibles à `GET /data/subscribe/{table}`. (REQ-219, REQ-258) La distribution des notifications s'appuie sur un fournisseur enfichable sélectionné selon le type de source : les sources PostgreSQL utilisent `LISTEN/NOTIFY`, les sources MongoDB les Change Streams et les sources Kafka des groupes de consommateurs. Le filtrage par sécurité au niveau des lignes et la validation de schéma s'appliquent quel que soit le fournisseur. Les sources WebSocket et RSS sont également prises en charge par le même endpoint. (REQ-338, REQ-342) [tool-verified: `provisa/api/data/subscribe.py:239`, `provisa/subscriptions/registry.py`, `provisa/api/app.py` `_rebuild_schemas`]

---

## Glossaire métier (REQ-1387)

Le glossaire métier fait correspondre les noms de champs physiques — tels qu'ils existent dans les bases de données sources — à un vocabulaire humain partagé. Chaque colonne enregistrée dans la couche sémantique reçoit automatiquement un terme. Aucune saisie manuelle n'est nécessaire pour peupler le glossaire ; les curateurs ajoutent définitions, relations et experts par-dessus ce que le système dérive.

### Comment les termes sont dérivés

Lorsque Provisa enregistre ou met à jour les colonnes d'une table, `normalize_term` (`provisa/core/glossary.py`) s'exécute sur chaque nom de colonne et produit une expression canonique. [tool-verified: `provisa/core/repositories/glossary.py:sync_table_refs`]

La normalisation applique cinq règles dans l'ordre :

1. Découpage aux frontières camelCase et sur les caractères séparateurs (`_`, `-`, `.`, `/`, espaces).
2. Passage du résultat en minuscules.
3. Expansion d'une table fixe d'abréviations (par exemple `cust` → `customer`, `amt` → `amount`, `dt` → `date`, `id` → `identifier`, `key` → `identifier`, `guid` → `identifier`).
4. Retrait d'un **jeton de substitution** final (`identifier`, `code`, `index` ou `reference`) — une colonne nommée d'après sa clé ou son code désigne le concept sous-jacent à travers une valeur de substitution, donc le terme doit être le concept lui-même. Le dernier jeton restant n'est jamais retiré.
5. Qualification d'une **expression trop générique** par le concept de la table. Lorsque l'expression normalisée complète est un mot d'attribut nu (`name`, `identifier`, `date`, `location`, `message`, `first name`, `last name`, et leurs semblables), le terme devient `<concept de la table> <expression>` — `employees.first_name` → `employee first name`, `orders.id` → `order identifier`. Un terme `name` unique partagé entre des tables sans rapport fusionnerait des sens distincts ; la qualification rattache chaque colonne au concept qui l'englobe. Le concept de la table est son nom métier, normalisé avec un nom de tête au singulier (`order_lines` → `order line`).

Les pseudo-colonnes de filtre natif (préfixées `_nf_`, ou portant `native_filter_type`) relèvent de la mécanique des paramètres de requête, non des champs métier, et ne dérivent aucun terme.

Comme `id`, `key`, `pk` et `sk` s'étendent tous en `identifier` avant le contrôle de substitution, trois noms de colonnes physiquement différents atterrissent exactement sur le même terme :

| Nom physique | Après normalisation |
| --- | --- |
| `cust_id` | `customer` |
| `customerId` | `customer` |
| `CUSTOMER_KEY` | `customer` |
| `txn_amt` | `transaction amount` |

Les trois premiers se rejoignent en un seul terme. `transaction amount` conserve ses deux jetons parce que `amount` n'est pas une substitution. Une colonne `id` nue — sans jeton précédent — ne peut pas être amputée ; elle se normalise en `identifier` pour que le terme ne soit pas vide. [tool-verified: `provisa/core/glossary.py:normalize_term`]

### Cycle de vie

Les termes sont **dérivés de l'appartenance à la couche sémantique**, non créés à la demande par les utilisateurs. Le dépôt de tables est l'unique chemin d'écriture : `sync_table_refs` s'exécute à l'intérieur de chaque upsert d'un ensemble de colonnes, et `sweep_refless_terms` s'exécute après tout chemin de suppression. [tool-verified: `provisa/core/repositories/glossary.py`]

**Quand une colonne est ajoutée :** Provisa cherche le terme normalisé par son nom. S'il existe déjà, la colonne reçoit une référence vers lui (et si le terme était obsolète, il est ranimé — `deprecated` repasse à `False`). Si aucun terme n'existe encore, il en crée un.

**Quand une colonne s'en va** (changement de schéma ou suppression de table) : sa référence est supprimée et le terme est **réglé** selon la règle retirer-ou-rendre-obsolète. Un terme ancré sans référence restante est retiré purement et simplement — avec ses arêtes et ses affectations d'experts — sauf si le retirer laisserait un terme abstrait déconnecté de tout terme ancré (aucun chemin à travers le graphe des termes). Dans ce cas, le terme est **rendu obsolète** (marqué `deprecated=True`) plutôt que supprimé, afin que l'ancrage du terme abstrait dans le graphe survive.

Les termes abstraits ne sont jamais retirés automatiquement ; ils existent hors du cycle de vie physique et ne sont supprimés qu'explicitement, via l'API d'administration.

**Ranimation :** si le nom normalisé d'un terme obsolète réapparaît (une colonne est réenregistrée), le terme est démarqué et ses références recommencent à s'accumuler.

### Endpoints de curation

Tous les endpoints se trouvent sous `/admin/glossary`. Ils exigent un accès `org_admin` et une organisation configurée. Chaque mutation déclenche une publication de métadonnées. [tool-verified: `provisa/api/admin/glossary_router.py`]

| Méthode | Chemin | Description |
| --- | --- | --- |
| `GET` | `/admin/glossary/terms` | Lister les termes. Paramètres de requête : `q` (recherche sur nom/définition), `include_deprecated` (`true` par défaut) |
| `GET` | `/admin/glossary/terms/{term_id}` | Obtenir le détail d'un terme : définition, références physiques, arêtes typées, experts |
| `POST` | `/admin/glossary/terms` | Créer un terme abstrait — vocabulaire d'utilisateur sans référence physique |
| `PATCH` | `/admin/glossary/terms/{term_id}` | Renommer, définir la définition ou basculer l'exclusion à l'export |
| `DELETE` | `/admin/glossary/terms/{term_id}` | Supprimer un terme sans référence physique |
| `POST` | `/admin/glossary/refs/move` | Déplacer une référence physique vers un autre terme (consolidation) |
| `POST` | `/admin/glossary/terms/{term_id}/edges` | Ajouter une arête de relation typée entre deux termes |
| `DELETE` | `/admin/glossary/terms/{term_id}/edges` | Retirer une arête (paramètres de requête : `to_term_id`, `rel_type`) |
| `POST` | `/admin/glossary/terms/{term_id}/experts` | Désigner un utilisateur comme expert ou auteur d'un terme |
| `DELETE` | `/admin/glossary/terms/{term_id}/experts/{user_id}` | Retirer la désignation d'expert/auteur d'un utilisateur |
| `POST` | `/admin/glossary/terms/{term_id}/definition/generate` | Rédiger une définition pour un terme avec le modèle d'IA de l'organisation — renvoie du texte seulement, rien n'est persisté avant enregistrement |
| `POST` | `/admin/glossary/definitions/generate` | Générer et persister une définition pour chaque terme qui n'en a pas — n'écrase jamais un texte rédigé par un humain |
| `POST` | `/admin/glossary/relationships/generate` | Proposer et persister des arêtes typées sur tout le glossaire avec le modèle d'IA de l'organisation |

**Corps de `POST /admin/glossary/terms` :**

```json
{"name": "revenue", "definition": "Recognized net revenue after returns and discounts."}
```

**Corps de `POST /admin/glossary/terms/{term_id}/edges` :**

```json
{"to_term_id": 42, "rel_type": "KIND_OF"}
```

Valeurs valides de `rel_type` : `KIND_OF`, `RELATED_TO`, `PART_OF`, `SYNONYM_OF`. [tool-verified: `provisa/core/glossary.py:TERM_EDGE_TYPES`]

**Corps de `POST /admin/glossary/terms/{term_id}/experts` :**

```json
{"user_id": "alice@example.com", "kind": "author"}
```

Valeurs valides de `kind` : `expert`, `author`. [tool-verified: `provisa/core/repositories/glossary.py:add_expert`]

**Corps de `POST /admin/glossary/refs/move` :**

```json
{"table_id": 7, "column_name": "cust_id", "to_term_id": 12}
```

Déplacer une référence règle le terme perdant selon la règle retirer-ou-rendre-obsolète. Employez-le pour consolider deux termes que la normalisation a gardés séparés — par exemple lorsqu'une source emploie une abréviation non standard, absente de la table d'expansion.

Supprimer un terme ancré (portant des références physiques) renvoie `400 glossary.invalid`. Retirez ou déplacez d'abord toutes ses références.

**`PATCH /admin/glossary/terms/{term_id}` — champ `export_excluded` :**

```json
{"export_excluded": true}
```

Poser `export_excluded` à `true` retient le terme hors de tous les instantanés d'export de métadonnées, quelles que soient ses références physiques ou sa nature abstraite. Le repasser à `false` restitue le terme à l'instantané à la publication suivante. Les données de curation (définition, arêtes, experts) ne sont pas touchées. [tool-verified: `provisa/core/repositories/glossary.py:set_export_excluded`, `provisa/api/admin/glossary_router.py:update_term`]

### Curation assistée par l'IA

Le modèle d'IA configuré par l'organisation peut rédiger des définitions et proposer des arêtes de relation sur tout le glossaire en une seule opération. Ces deux actions groupées exigent un accès `org_admin` et une organisation configurée.

**`POST /admin/glossary/definitions/generate`**

Parcourt chaque terme du glossaire, ignore ceux qui ont déjà une définition et appelle le modèle d'IA de l'organisation pour en rédiger une pour chaque terme restant. La rédaction est persistée immédiatement — contrairement à l'endpoint de rédaction par terme (`POST /admin/glossary/terms/{term_id}/definition/generate`), il n'y a pas d'étape d'édition. Les définitions rédigées par un humain ne sont jamais écrasées : le garde-fou est `if summary["definition"]: continue` avant tout appel au modèle. Une seule notification de publication couvre le lot entier. [tool-verified: `provisa/api/admin/glossary_router.py:generate_all_definitions`]

Réponse :

```json
{"generated": 12}
```

`generated` est le nombre de termes ayant reçu une nouvelle définition. Il vaut zéro lorsque chaque terme en a déjà une.

**`POST /admin/glossary/relationships/generate`**

Envoie la liste complète des termes au modèle d'IA de l'organisation avec une invite qui précise les dix types d'arêtes autorisés (`KIND_OF`, `PART_OF`, `SYNONYM_OF`, `RELATED_TO`, `VALID_VALUE_OF`, `DERIVED_FROM`, `REPLACES`, `PREFERRED_TERM_FOR`, `TRANSLATION_OF`, `ANTONYM_OF`) et ne demande que des propositions sûres. Le modèle répond par un tableau JSON ; chaque entrée est validée avant toute écriture : noms de termes inconnus, arêtes réflexives et types d'arêtes hors de l'énumération fermée sont écartés en silence. Les propositions valides sont insérées de façon idempotente — relancer l'action ne duplique pas d'arêtes. Une seule notification de publication couvre le lot. L'endpoint renvoie `{"added": 0}` immédiatement lorsque le glossaire compte moins de deux termes non obsolètes. [tool-verified: `provisa/api/admin/glossary_router.py:generate_relationships`]

Réponse :

```json
{"added": 5}
```

`added` est le nombre d'arêtes écrites. Une arête qui existait déjà compte quand même — l'upsert réussit, mais les données de l'arête ne changent pas.

### Outil MCP `search_terms`

```
search_terms(query, role=None, limit=25)
```

Recherche dans les noms et définitions des termes par sous-chaîne insensible à la casse, jusqu'à `limit` résultats. Chaque résultat est le détail complet du terme : `name`, `definition`, `is_abstract`, `deprecated`, les références physiques (avec `source_id`, `schema_name`, `table_name`, `column_name`), les arêtes typées et les affectations d'experts. [tool-verified: `provisa/api/mcp/server.py:236-244`, `provisa/core/repositories/glossary.py:search_terms`]

Employez `search_terms` avant d'écrire du SQL pour trouver, par son nom, chaque champ physique qui représente un concept. Par exemple, rechercher `"order date"` renvoie le terme et toutes les colonnes `order_dt`, `orderDate`, `ORDER_DATE` de chaque table enregistrée.

### Export de métadonnées

Le graphe des termes du glossaire est inclus dans chaque `MetadataSnapshot` construit par `build_snapshot`. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]

L'export applique les mêmes filtres que le reste de l'instantané :

- Un terme marqué `export_excluded` est retenu purement et simplement — quelles que soient ses références physiques, sa nature abstraite ou la configuration du catalogue de l'organisation. [tool-verified: `provisa/api/metadata_export/builder.py:_glossary_assets`]
- Un terme ancré n'est publié que si au moins une de ses références physiques porte sur une colonne qui passe à la fois le filtre **Data Product** (le drapeau `data_product` de la table doit valoir `true`) et le filtre de colonnes **techniques** (les colonnes étiquetées `technical` sont retenues).
- Un terme ancré dont toutes les références sont retenues par ces filtres est retenu avec elles.
- Les termes abstraits sont publiés sans condition — ils relèvent du vocabulaire d'utilisateur, sans lien avec des colonnes physiques.
- Une arête entre deux termes n'est publiée que si ses deux termes d'extrémité le sont.

Chaque adaptateur d'éditeur publie le graphe des termes nativement, dans un conteneur de glossaire appartenant à Provisa qu'il crée de façon idempotente — jamais dans un glossaire de catalogue existant :

| Fournisseur | Conteneur | Termes | Relations | Mise hors service |
| --- | --- | --- | --- | --- |
| Apache Atlas | « Provisa Glossary » (API de glossaire) | termes de glossaire, définition sur `longDescription` | KIND_OF → `isA`, SYNONYM_OF → `synonyms`, RELATED_TO/PART_OF → `seeAlso` | marqueur `[DEPRECATED]` dans shortDescription |
| Atlan | glossaire Provisa par qualifiedName stable | `longDescription` (jamais `userDescription`, édité par des humains) | même correspondance qu'Atlas | `certificateStatus = DEPRECATED` |
| DataHub | `urn:li:glossaryNode:provisa.<org>` | aspect `glossaryTermInfo` par terme | KIND_OF → Inherits, PART_OF → Contains (inversé), RELATED_TO/SYNONYM_OF → termes liés | aspect de dépréciation ; les renommages suivent la succession d'URN |
| OpenMetadata | glossaire Provisa via `/v1/glossaries` | PUT indexé par fqn, les renommages rebasculent par PATCH sur l'UUID stocké | KIND_OF → hiérarchie parente native, SYNONYM_OF → `synonyms`, autres → `relatedTerms` | `entityStatus` |
| Collibra | domaine de type glossaire « Provisa Glossary » | actifs Business Term via l'API d'import | types de relations Business Term natifs | statut de l'actif |

C'est la propriété qui fait le lien, pas le nom : l'identifiant côté éditeur de chaque terme publié est consigné dans `catalog_bindings` sous l'URN du terme (`provisa://<org>/terms/<name>`), et Provisa ne modifie ou ne supprime un élément de glossaire côté éditeur que lorsqu'il détient ce lien (ou que l'élément vit dans le conteneur appartenant à Provisa qu'il a créé). Un élément de glossaire sans lien Provisa provient du système externe et n'est jamais touché ; les mises à jour lisent et fusionnent, si bien que les champs ajoutés par un intendant sur les termes propres à Provisa survivent ; rien n'est supprimé quand un terme quitte l'instantané. Les affectations terme-vers-actif faites par les intendants restent la propriété de l'extérieur — aucun adaptateur n'écrit d'affectation terme-vers-actif (la publication d'affectations produites par Provisa est une suite explicite). Sur Collibra en particulier, la sûreté sous la sémantique REPLACE de l'API d'import repose sur le confinement : la charge utile ne mentionne que des actifs situés dans le domaine de glossaire Provisa et des instances de relations entre termes Provisa, si bien que les glossaires des intendants et leurs relations restent hors d'atteinte. [tool-verified: `provisa/api/metadata_export/atlan.py`, `provisa/api/metadata_export/datahub.py`, `provisa/api/metadata_export/atlas.py`, `provisa/api/metadata_export/openmetadata.py`]
