# Admin API

L'Admin API est un endpoint Strawberry GraphQL sur `POST /admin/graphql` (REQ-533). Elle nécessite un rôle superutilisateur ou admin (REQ-125, REQ-060) et est distincte de l'endpoint GraphQL de données (REQ-533).

## Authentification

Transmettez vos identifiants dans l'en-tête `Authorization` en utilisant le fournisseur d'authentification standard de Provisa (REQ-120)&nbsp;:

```yaml
Authorization: Bearer <token>
```

L'accès admin est régi par la capacité `admin` attribuée à un rôle (REQ-060, REQ-042).

### Jetons d'accès personnels

Un jeton d'accès personnel est accepté partout où un jeton bearer l'est, y compris sur ce point d'entrée. L'émission et la révocation se font en libre-service&nbsp;: c'est la credential propre du porteur, elle se trouve donc sur le profil de l'utilisateur dans l'interface d'administration plutôt que sous une page d'administrateur, aux côtés du départ d'une organisation et de la suppression du compte. Un administrateur ne crée pas de jetons pour le compte d'autrui. (REQ-1263)

| Route | Effet |
| ------- | -------- |
| `POST /auth/tokens` | Crée un jeton pour l'appelant. Corps&nbsp;: `name`, et en option `role_id`, `scopes`, `expires_in_days` (1–366). La réponse est le seul endroit où le secret apparaît jamais |
| `GET /auth/tokens` | Les jetons actifs de l'appelant dans cette organisation&nbsp;: préfixe d'affichage, nom, horodatages du cycle de vie et le hachage qui identifie un jeton pour la révocation. Jamais une credential utilisable |
| `DELETE /auth/tokens/{token_hash}` | Révoque l'un des jetons de l'appelant. 404 s'il ne lui appartient pas ou s'il est déjà révoqué |

Omettre `role_id` laisse le jeton se résoudre au rôle que détient son propriétaire&nbsp;; en nommer un restreint le jeton en deçà de son propriétaire. La révocation survient aussi implicitement&nbsp;: retirer l'appartenance d'un utilisateur à une organisation révoque ses jetons pour cette organisation. Pour la credential elle-même, voir [Modèle de sécurité](security.md#personal-access-tokens).

## Capacités

### Gestion de la configuration

Téléchargez la configuration en cours d'exécution (REQ-164)&nbsp;:

```http
GET /admin/config
```

Renvoie le fichier `config.yaml` complet au format YAML. Envoyez une nouvelle configuration (REQ-164)&nbsp;:

```http
PUT /admin/config
```

Provisa valide le YAML, recharge les catalogues et régénère les schémas (REQ-012, REQ-253). Aucun redémarrage n'est requis.

### Paramètres d'exécution

Lisez et écrivez les paramètres de la plateforme au moment de l'exécution sans modifier le fichier de configuration (REQ-165)&nbsp;:

```http
GET  /admin/settings
PUT  /admin/settings
```

La surface de paramètres couvre la redirection des résultats volumineux, l'échantillonnage par défaut et la limite de lignes, le TTL du cache de réponses, la convention de nommage, le suivi automatique des clés étrangères de relations, le DSN du stockage de matérialisation, la mémoire du moteur de fédération (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`), ainsi que toute la surface de réglage du pipeline de traçage OpenTelemetry (REQ-1082). Les limites de parcours GraphQL distant et les paramètres de niveau intermédiaire (warm-tier)/cache de lecture sont également exposés (REQ-1081, REQ-1083).

Posture de sécurité — `security.mode` (`standard` | `high`) — appliquée au redémarrage (REQ-1079)&nbsp;:

```http
GET  /admin/security
PUT  /admin/security
```

Attribution des modèles d'IA, registre des modèles d'embedding/de vecteurs, et limite de débit NL — appliqués au redémarrage (REQ-1080)&nbsp;:

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

L'onglet de chiffrement de l'admin dérive sa liste de fournisseurs en direct depuis le registre de chiffrement&nbsp;; les fournisseurs indisponibles apparaissent mais ne sont pas sélectionnables (REQ-1091).

`GET`/`HEAD /health` et `GET /setup/status` sont toujours non authentifiés — ils contournent l'exigence `Authorization: Bearer` même lorsqu'un fournisseur d'authentification est configuré (REQ-539).

### Éditeur de relations

Listez les relations (REQ-166)&nbsp;:

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

Créez une relation (REQ-019)&nbsp;:

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

Déclenchez l'analyse des clés étrangères propulsée par Claude via REST (REQ-167, REQ-018)&nbsp;:

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Renvoie les candidats de clé étrangère classés par niveau de confiance. Acceptez un candidat&nbsp;:

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspection de schéma

Parcourez les tables publiées dans toutes les sources (REQ-008)&nbsp;:

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

### Gestion des vues

Enregistrez une vue matérialisée (REQ-133, REQ-135)&nbsp;:

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

Déclenchez une actualisation manuelle (REQ-135)&nbsp;:

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Enregistrement de sources de graphe

Les sources Neo4j et SPARQL sont enregistrées via des endpoints REST (et non l'Admin API GraphQL) (REQ-295, REQ-297)&nbsp;:

**Neo4j&nbsp;:**

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

**SPARQL&nbsp;:**

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

L'Admin API inclut GraphiQL sur `GET /admin/graphql` dans le navigateur (REQ-622). Utilisez-le pour explorer l'ensemble du schéma admin de façon interactive.

## Vues de gestion du domaine ops (REQ-1386)

Huit vues SQL sont installées dans le domaine intégré `ops` à chaque installation. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Elles exposent le journal d'audit des requêtes sous forme de tables gouvernées — interrogeables en SQL (pgwire), GraphQL et Cypher, sous les mêmes règles d'accès au domaine, de RLS et de masquage que n'importe quelle table métier.

`org_admin` est désigné intendant du domaine ops au moment de l'amorçage, si bien que le domaine n'apparaît jamais comme une lacune de gouvernance dans `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Vue | Ce qu'elle répond |
| --- | --- |
| `usage_ranking` | Nombre de requêtes et utilisateurs distincts par table enregistrée&nbsp;; les tables sans aucun accès ressortent comme candidates à l'abandon |
| `deprecated_usage` | Chaque accès à une table ou une colonne portant l'étiquette `deprecated` — les consommateurs actifs qui empêchent un retrait sans risque |
| `pii_access` | Chaque accès à une table ou une colonne portant l'étiquette `pii`&nbsp;: qui a interrogé, sous quel rôle, via quelle surface |
| `policy_denials` | Toutes les tentatives d'accès rejetées par la gouvernance (HTTP 401/403) |
| `surface_mix` | Nombre quotidien de requêtes et utilisateurs distincts par surface de protocole (SQL, GraphQL, Cypher, gRPC, etc.) |
| `query_health` | Nombre quotidien d'erreurs et latence moyenne/maximale par surface |
| `stale_metadata` | Tables et colonnes sans description&nbsp;; domaines sans intendant |
| `join_hotspots` | Paires de tables les plus souvent interrogées ensemble — candidates à la matérialisation ou à la mise en cache |

Deux limites s'appliquent aujourd'hui. La granularité est celle de la table — le journal d'audit enregistre `table_ids`, pas les colonnes précises consultées. Le texte des requêtes est chiffré (REQ-689) et exclu de toutes ces vues&nbsp;; il n'est accessible que par le chemin de déchiffrement administrateur autorisé. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Un rôle doit avoir accès au domaine `ops` pour que ces vues soient visibles. Accordez-le comme vous accordez l'accès à tout autre domaine.

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

Les mêmes requêtes s'exécutent en GraphQL ou en Cypher sur n'importe quel transport gouverné — pgwire, Arrow Flight ou Bolt. [inferred from governed-surface design]

## Visionneuse de rapports (REQ-1390)

La visionneuse de rapports se trouve à `/admin/reports`. Les rôles dépourvus de la capacité `observability` ne peuvent pas y accéder.

Le panneau de gauche liste chaque table enregistrée du domaine `ops`, triée par alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Les huit vues de gestion installées y apparaissent automatiquement. Cliquez sur un rapport pour le charger dans la visionneuse de données gouvernée à droite.

**Ajouter un rapport personnalisé.** Le bouton «&nbsp;Ajouter un rapport&nbsp;» ouvre une boîte de dialogue. Fournissez un nom, une description facultative et une instruction SELECT. À l'enregistrement, la vue est déclarée comme table dérivée gouvernée dans le domaine `ops` — cataloguée, soumise au contrôle d'accès et interrogeable via toutes les surfaces, aux côtés des vues installées. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Suppression.** L'icône de corbeille n'apparaît que pour les rapports personnalisés. Les vues de gestion installées ne peuvent pas être supprimées depuis cette interface. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Aperçu de table (REQ-1392)

Dépliez n'importe quelle ligne de table sur la page Tables. Le bouton **Aperçu** ouvre une fenêtre modale occupant 90&nbsp;% de la largeur, avec les données gouvernées en direct de la table. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Les tables adossées à des API comportant des paramètres de chemin obligatoires bloquent l'aperçu tant que ces valeurs ne sont pas fournies. Un formulaire en ligne recueille chaque paramètre obligatoire avant la première requête&nbsp;; les paramètres de requête facultatifs figurent dans le même formulaire. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check; "paramsRequired" message shown when `activeParams == null`]

## Visionneuse de données gouvernée (REQ-1391)

Le même composant de visionneuse alimente la fenêtre d'aperçu et la visionneuse de rapports. Son comportement est identique dans les deux contextes.

**Pagination côté serveur.** Chaque page est son propre `SELECT *` gouverné avec `LIMIT 101 OFFSET n`. 100 lignes apparaissent par page&nbsp;; la 101e indique s'il en existe d'autres. Le jeu de données complet n'est jamais chargé dans le navigateur. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Filtres et tris poussés vers la source.** Chaque en-tête de colonne comporte un champ de filtre. Les termes de filtre deviennent des prédicats `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`&nbsp;; les clics de tri produisent des clauses `ORDER BY`. Les deux partent vers la base de données — un filtre sur une table d'un milliard de lignes parcourt la source, pas les 100 lignes affichées devant vous. [tool-verified: `nativeParams.ts:53-70`]

**Regroupement multi-niveaux.** L'icône Calques d'un en-tête de colonne ajoute cette colonne au regroupement. Les colonnes de groupe ouvrent l'`ORDER BY`, de sorte que les membres d'un groupe se retrouvent sur la même page que leur en-tête d'une page à l'autre. Les colonnes de clé primaire sont ajoutées en fin de liste comme départage stable. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Les lignes d'en-tête de groupe sont repliables&nbsp;; les replier masque les membres sans émettre de nouvelle requête. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Choix persistants.** Les réglages de filtre, de tri et de regroupement sont conservés dans `localStorage` sous `provisa.grid.table:<domain>.<table>` et restaurés à la visite suivante. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Export.** Téléchargez la page courante en CSV, ou copiez-la dans le presse-papiers sous forme de texte séparé par des tabulations. L'export ne couvre que la page visible. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
