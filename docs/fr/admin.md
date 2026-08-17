# API Admin

L'API admin est un endpoint GraphQL Strawberry à `POST /admin/graphql` (REQ-533). Elle requiert un rôle superuser ou admin (REQ-125, REQ-060) et est distincte de l'endpoint GraphQL de données (REQ-533).

## Authentification

Passez vos identifiants dans l'en-tête `Authorization` en utilisant le fournisseur d'authentification standard de Provisa (REQ-120) :

```yaml
Authorization: Bearer <token>
```

L'accès admin est régi par la capacité `admin` assignée à un rôle (REQ-060, REQ-042).

### Jetons d'accès personnels

Un jeton d'accès personnel est accepté partout où un jeton porteur (bearer token) l'est, y compris sur cet endpoint. Son émission et sa révocation sont en libre-service — c'est l'identifiant propre au détenteur du jeton, il vit donc dans le profil de l'utilisateur dans l'UI admin plutôt que sous une page admin, aux côtés de la sortie d'une organisation et de la suppression du compte. Un administrateur ne génère pas de jetons au nom de quelqu'un d'autre. (REQ-1263)

| Route | Effet |
| ------- | -------- |
| `POST /auth/tokens` | Génère un jeton pour l'appelant. Corps : `name`, `role_id` optionnel, `scopes`, `expires_in_days` (1–366). La réponse est le seul endroit où le secret apparaît jamais |
| `GET /auth/tokens` | Les jetons actifs de l'appelant dans cette org — préfixe d'affichage, nom, horodatages de cycle de vie, et le hash qui identifie un jeton pour révocation. Jamais un identifiant fonctionnel |
| `DELETE /auth/tokens/{token_hash}` | Révoque l'un des jetons de l'appelant. 404 s'il ne lui appartient pas ou est déjà révoqué |

Omettre `role_id` laisse le jeton se résoudre vers quel que soit le rôle détenu par son propriétaire ; en nommer un restreint le jeton en deçà de son propriétaire. La révocation se produit aussi implicitement : retirer l'appartenance d'un utilisateur à une organisation révoque ses jetons pour cette organisation. Voir [Security Model](security.md#jetons-dacces-personnels) pour l'identifiant lui-même.

## Capacités

### Gestion de la configuration

Télécharger la configuration en cours d'exécution (REQ-164) :

```http
GET /admin/config
```

Retourne le `config.yaml` complet sous forme de fichier YAML. Téléverser une nouvelle configuration (REQ-164) :

```http
PUT /admin/config
```

Provisa valide le YAML, recharge les catalogues, et régénère les schémas (REQ-012, REQ-253). Aucun redémarrage requis.

### Paramètres d'exécution

Lire et écrire les paramètres de la plateforme en cours d'exécution sans modifier le fichier de configuration (REQ-165) :

```http
GET  /admin/settings
PUT  /admin/settings
```

La surface des paramètres couvre la redirection de gros résultats, l'échantillonnage par défaut et la limite de lignes, le TTL du cache de réponse, la convention de nommage, le suivi automatique des clés étrangères de relation, le DSN du magasin de matérialisation, la mémoire du moteur de fédération (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), et l'intégralité de la surface de réglage du pipeline de traçage OpenTelemetry (REQ-1082). Les limites de traversée GraphQL distant et les paramètres de palier chaud/cache de lecture sont aussi exposés (REQ-1081, REQ-1083).

Posture de sécurité — `security.mode` (`standard` | `high`) — appliquée au redémarrage (REQ-1079) :

```http
GET  /admin/security
PUT  /admin/security
```

Assignations de modèles IA, registre de modèles d'embedding/vecteurs, et limite de débit NL — appliquées au redémarrage (REQ-1080) :

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

L'onglet de chiffrement admin dérive sa liste de fournisseurs en direct depuis le registre de chiffrement ; les fournisseurs indisponibles apparaissent mais ne sont pas sélectionnables (REQ-1091).

`GET`/`HEAD /health` et `GET /setup/status` sont toujours non authentifiés — ils contournent l'exigence `Authorization: Bearer` même quand un fournisseur d'authentification est configuré (REQ-539).

### Éditeur de relations

Lister les relations (REQ-166) :

```graphql
query {
  relationships {
    id
    sourceTableId
    targetTableId
    sourceColumn
    targetColumn
    cardinality
    materialize
  }
}
```

Créer une relation (REQ-019) :

```graphql
mutation {
  upsertRelationship(input: {
    id: "orders-to-customers"
    sourceTableId: "orders"
    targetTableId: "customers"
    sourceColumn: "customer_id"
    targetColumn: "id"
    cardinality: "many_to_one"
  }) {
    success
  }
}
```

### Découverte de relations par IA

Déclencher l'analyse de clés étrangères propulsée par Claude via REST (REQ-167, REQ-018) :

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Retourne les candidats FK classés par confiance. Accepter un candidat :

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspection de schéma

Parcourir les tables publiées à travers toutes les sources (REQ-008) :

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
      writableBy
    }
  }
}
```

### Vérification des dépendances de colonne (REQ-1484)

Avant d'enregistrer une modification de table qui renomme l'alias SQL d'une colonne ou supprime une colonne, demandez ce qui d'autre y fait référence :

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Renommer un alias casse chaque artefact rédigé par rapport au nom exposé — vues, MV, expressions de métriques, prédicats RLS, contrats DQ. Supprimer une colonne casse ceux-ci plus les artefacts qui stockent le `column_name` physique : relations, liaisons de glossaire, assignations de tags. `breaksOn` indique lequel. La page Tables exécute cela à l'enregistrement et affiche le résultat sous forme de boîte de dialogue consultative. Voir [Lineage](lineage.md) pour ce que la requête couvre et ce qu'elle ne peut pas couvrir.

### Gestion des vues

Enregistrer une vue matérialisée (REQ-133, REQ-135) :

```graphql
mutation {
  registerTable(input: {
    viewSql: "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON o.customer_id = c.id"
    mvRefreshInterval: 300
    materialize: true
  }) {
    success
  }
}
```

Déclencher un rafraîchissement manuel (REQ-135) :

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Enregistrement de sources graphe

Les sources Neo4j et SPARQL sont enregistrées via des endpoints REST (pas l'API GraphQL admin) (REQ-295, REQ-297) :

**Neo4j :**

```bash
# 1. Register the Neo4j source
curl -X POST http://localhost:8001/admin/sources/neo4j \
  -H "Content-Type: application/json" \
  -d '{"source_id": "graph", "host": "neo4j", "port": 7474, "database": "neo4j"}'

# 2. Preview a Cypher query (validates scalar projections)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/preview \
  -H "Content-Type: application/json" \
  -d '{"cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age"}'

# 3. Register a table (runs preview+validate automatically)
curl -X POST http://localhost:8001/admin/sources/neo4j/graph/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "people", "cypher": "MATCH (p:Person) RETURN p.name AS name, p.age AS age", "ttl": 300}'
```

**SPARQL :**

```bash
# 1. Register the SPARQL source
curl -X POST http://localhost:8001/admin/sources/sparql \
  -H "Content-Type: application/json" \
  -d '{"source_id": "kg", "endpoint_url": "http://fuseki:3030/ds/sparql"}'

# 2. Register a table (probes endpoint and infers columns)
curl -X POST http://localhost:8001/admin/sources/sparql/kg/tables \
  -H "Content-Type: application/json" \
  -d '{"table_name": "products", "sparql_query": "SELECT ?name ?category WHERE { ?p a :Product ; :name ?name ; :category ?category . }", "ttl": 600}'
```

Une fois enregistrées, les tables apparaissent dans le schéma GraphQL et sont interrogeables comme toute autre source (REQ-016).

## GraphiQL

L'API admin est livrée avec GraphiQL à `GET /admin/graphql` dans le navigateur (REQ-622). Utilisez-le pour explorer le schéma admin complet de manière interactive.

## Vues de gestion du domaine ops (REQ-1386)

Huit vues SQL sont préchargées dans le domaine `ops` intégré à chaque installation. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Elles exposent le journal d'audit des requêtes comme des tables gouvernées — interrogeables via SQL (pgwire), GraphQL, et Cypher, sous les mêmes règles d'accès au domaine, RLS, et masquage que toute table métier.

`org_admin` est désigné comme steward du domaine ops au moment du préchargement, de sorte que le domaine n'apparaît jamais comme une lacune de gouvernance dans `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Vue | À quoi elle répond |
| --- | --- |
| `usage_ranking` | Nombre de requêtes et d'utilisateurs distincts par table enregistrée ; les tables à zéro accès apparaissent comme candidates à la dépréciation |
| `deprecated_usage` | Chaque accès à une table ou colonne portant le tag `deprecated` — les consommateurs actifs bloquant une suppression sûre |
| `pii_access` | Chaque accès à une table ou colonne portant le tag `pii` : qui l'a interrogée, sous quel rôle, sur quelle surface |
| `policy_denials` | Toutes les tentatives d'accès que la gouvernance a rejetées (HTTP 401/403) |
| `surface_mix` | Nombre de requêtes et d'utilisateurs distincts quotidiens par surface de protocole (SQL, GraphQL, Cypher, gRPC, etc.) |
| `query_health` | Nombre d'erreurs et latence moyenne/maximale quotidiennes par surface |
| `stale_metadata` | Tables et colonnes sans description ; domaines sans steward |
| `join_hotspots` | Paires de tables interrogées conjointement le plus souvent — candidates pour la matérialisation ou la mise en cache |

Deux limites s'appliquent aujourd'hui. La granularité est au niveau table — le journal d'audit enregistre `table_ids`, pas les colonnes individuelles accédées. Le texte de requête est chiffré (REQ-689) et exclu de chaque vue ici ; il n'est accessible que via le chemin de déchiffrement admin autorisé. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Un rôle nécessite un accès au domaine `ops` avant que ces vues soient visibles. Accordez-le de la même manière que vous accordez l'accès à tout autre domaine.

```sql
-- Which tables have never been queried?
SELECT table_name, domain_id
FROM ops.usage_ranking
WHERE query_count = 0;

-- Who accessed PII-tagged data in the last 7 days?
SELECT user_id, role_id, source, pii_column, logged_at
FROM ops.pii_access
WHERE logged_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY logged_at DESC;

-- Where does traffic originate by protocol?
SELECT source, day, query_count, distinct_users
FROM ops.surface_mix
ORDER BY day DESC, query_count DESC;
```

Les mêmes requêtes s'exécutent en GraphQL ou en Cypher sur tout transport gouverné — pgwire, Arrow Flight, ou Bolt. [inferred from governed-surface design]

## Visualiseur de rapports (REQ-1390)

Le visualiseur de rapports se trouve à `/admin/reports`. Les rôles sans la capacité `observability` ne peuvent pas y accéder.

Le panneau de gauche liste chaque table enregistrée dans le domaine `ops`, triée par alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Les huit vues de gestion préchargées y apparaissent automatiquement. Cliquez sur n'importe quel rapport pour le charger dans le visualiseur de données gouvernées à droite.

**Ajouter un rapport personnalisé.** Le bouton « Add report » ouvre une boîte de dialogue. Fournissez un nom, une description optionnelle, et une instruction SELECT. L'enregistrement inscrit la vue comme une table dérivée gouvernée dans le domaine `ops` — cataloguée, contrôlée en accès, et interrogeable via toute surface aux côtés des vues préchargées. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Suppression.** L'icône de corbeille n'apparaît que pour les rapports personnalisés. Les vues de gestion préchargées ne peuvent pas être supprimées depuis cette interface. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Prévisualisation de table (REQ-1392)

Développez n'importe quelle ligne de table sur la page Tables. Le bouton **Preview** ouvre une modale de 90 % de largeur avec les données gouvernées en direct de la table. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Les tables adossées à des API avec des paramètres de chemin requis bloquent la prévisualisation jusqu'à ce que ces valeurs soient fournies. Un formulaire en ligne collecte chaque paramètre requis avant l'exécution de la première requête ; les paramètres de requête optionnels apparaissent dans le même formulaire. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Visualiseur de données gouvernées (REQ-1391)

Le même composant de visualisation alimente la modale de prévisualisation et le visualiseur de rapports. Son comportement est identique dans les deux contextes.

**Pagination côté serveur.** Chaque page est son propre `SELECT *` gouverné avec `LIMIT 101 OFFSET n`. 100 lignes apparaissent par page ; la 101e signale s'il en existe davantage. Le jeu de données complet n'est jamais chargé dans le navigateur. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Filtres et tris repoussés en amont (pushed-down).** Chaque en-tête de colonne a un champ de filtre. Les termes de filtre deviennent des prédicats `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` ; les clics de tri produisent des clauses `ORDER BY`. Les deux vont à la base de données — un filtre sur une table d'un milliard de lignes balaie la source, pas la page de 100 lignes devant vous. [tool-verified: `nativeParams.ts:53-70`]

**Regroupement multi-niveaux.** L'icône Layers dans n'importe quel en-tête de colonne bascule cette colonne dans le regroupement. Les colonnes de regroupement mènent le `ORDER BY` de sorte que les membres d'un groupe atterrissent sur la même page que leur en-tête, à travers les limites de page. Les colonnes de clé primaire sont ajoutées comme départage stable. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Les lignes d'en-tête de groupe sont repliables ; le repliement masque les membres sans émettre de nouvelle requête. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Choix persistants.** Les paramètres de filtre, tri, et regroupement persistent dans `localStorage` sous `provisa.grid.table:<domain>.<table>` et se restaurent à la visite suivante. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Export.** Téléchargez la page courante en CSV, ou copiez-la dans le presse-papiers sous forme de texte séparé par tabulations. L'export ne couvre que la page visible. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
