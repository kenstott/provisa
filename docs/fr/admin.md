# API d'administration

L'API d'administration est un endpoint GraphQL Strawberry à `POST /admin/graphql` (REQ-533). Elle exige un rôle superuser ou admin (REQ-125, REQ-060) et est distincte de l'endpoint GraphQL de données (REQ-533).

## Authentification

Transmettez vos identifiants dans l'en-tête `Authorization` en utilisant le fournisseur d'authentification Provisa standard (REQ-120) :

```yaml
Authorization: Bearer <token>
```

L'accès administrateur est régi par la capacité `admin` affectée à un rôle (REQ-060, REQ-042).

### Jetons d'accès personnels

Un jeton d'accès personnel est accepté partout où un jeton bearer l'est, y compris sur cet endpoint. L'émettre et le révoquer relève du libre-service — c'est l'identifiant propre du détenteur du jeton, il vit donc sur le profil de l'utilisateur dans l'interface d'administration plutôt que sous une page d'administration, aux côtés du départ d'une organisation et de la suppression du compte. Un administrateur ne frappe pas de jetons pour le compte d'autrui. (REQ-1263)

| Route | Effet |
| ------- | -------- |
| `POST /auth/tokens` | Frappe un jeton pour l'appelant. Corps : `name`, en option `role_id`, `scopes`, `expires_in_days` (1–366). La réponse est le seul endroit où le secret apparaîtra jamais |
| `GET /auth/tokens` | Les jetons actifs de l'appelant dans cette organisation — préfixe d'affichage, nom, horodatages de cycle de vie et l'empreinte qui identifie un jeton en vue de sa révocation. Jamais un identifiant utilisable |
| `DELETE /auth/tokens/{token_hash}` | Révoque l'un des jetons de l'appelant. 404 lorsqu'il ne lui appartient pas ou a déjà été révoqué |

Omettre `role_id` laisse le jeton se résoudre vers le rôle que détient son propriétaire ; en nommer un restreint le jeton en deçà de son propriétaire. La révocation a lieu aussi implicitement : retirer l'appartenance d'un utilisateur à une organisation révoque ses jetons pour celle-ci. Voir [Modèle de sécurité](security.md#jetons-dacces-personnels) pour l'identifiant lui-même.

## Capacités

### Gestion de la configuration

Téléchargez la configuration en cours d'exécution (REQ-164) :

```http
GET /admin/config
```

Renvoie le `config.yaml` complet sous forme de fichier YAML. Téléversez une nouvelle configuration (REQ-164) :

```http
PUT /admin/config
```

Provisa valide le YAML, recharge les catalogues et régénère les schémas (REQ-012, REQ-253). Aucun redémarrage requis.

### Paramètres d'exécution

Lisez et écrivez les paramètres de plateforme à l'exécution sans éditer le fichier de configuration (REQ-165) :

```http
GET  /admin/settings
PUT  /admin/settings
```

La surface des paramètres couvre la redirection des grands résultats, l'échantillonnage et la limite de lignes par défaut, le TTL du cache de réponses, la convention de nommage, le suivi automatique des clés étrangères de relations, le DSN du magasin de matérialisation, la mémoire du moteur de fédération (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) ainsi que l'ensemble de la surface de réglage du pipeline de traçage OpenTelemetry (REQ-1082). Les limites de traversée GraphQL distante et les paramètres de niveau tiède / cache de lecture sont également exposés (REQ-1081, REQ-1083).

Posture de sécurité — `security.mode` (`standard` | `high`) — appliquée au redémarrage (REQ-1079) :

```http
GET  /admin/security
PUT  /admin/security
```

Les affectations de modèles d'IA, le registre des modèles d'embedding et de vecteurs ainsi que la limite de débit du langage naturel prennent effet à la requête suivante, sans redémarrage (REQ-1349) : [tool-verified: `provisa/api/admin/ai_models_router.py:38-39`]

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

L'onglet de chiffrement de l'administration dérive sa liste de fournisseurs en direct du registre de chiffrement ; les fournisseurs indisponibles y apparaissent mais ne sont pas sélectionnables (REQ-1091).

`GET`/`HEAD /health` et `GET /setup/status` sont toujours non authentifiés — ils contournent l'exigence `Authorization: Bearer` même lorsqu'un fournisseur d'authentification est configuré (REQ-539).

### Moteur de fédération

Lisez ou changez le moteur qu'utilise le déploiement (REQ-916) :

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` renvoie la clé du moteur actif et les champs de configuration dont il a besoin. `PUT` accepte un corps comportant `engine` (la clé) et tout champ propre au moteur ; la sélection est persistée dans la configuration de la plateforme et prend effet au prochain redémarrage du service. [tool-verified: `provisa/api/admin/settings_router.py:730-829`]

### Éditeur de relations

Lister les relations (REQ-166) :

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

Créer une relation (REQ-019) :

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

Déclarer une relation adossée à une jonction (REQ-1586) :

```graphql
mutation {
  upsertRelationship(input: {
    id: "pets-bonded-pair"
    sourceTableId: "pets"
    targetTableId: "pets"
    sourceColumn: "id"
    targetColumn: "id"
    cardinality: "one-to-many"
    viaTable: "pet_companions"
    viaSourceColumn: "pet_id"
    viaTargetColumn: "companion_pet_id"
    viaTypeColumn: "companion_type"
    viaTypeValue: "bonded pair"
    viaLabelSource: "column"
  }) {
    success
  }
}
```

Une table associative est déclarée comme arête, jamais découverte. `viaTable` nomme une table enregistrée ; ses deux colonnes clés portent l'arête, et chaque colonne restante devient un attribut de la relation, filtrable comme n'importe quel autre champ. `viaTypeColumn` / `viaTypeValue` scindent une même table de jonction en plusieurs types d'arêtes — trois lignes de `pet_companions` dont `companion_type` vaut `bonded pair`, `littermate` et `shares enclosure` sont trois relations distinctes sur la même paire de tables.

`viaLabelSource` désigne d'où vient le nom exposé, et les trois formes sont mises en UPPER_SNAKE_CASE pour Cypher : `column` utilise `viaTypeValue` (`BONDED_PAIR`), `table` utilise le nom propre de la table de jonction (`PET_COMPANIONS`), `fixed` utilise l'`alias` déclaré. Une table de jonction déclarée ainsi est une arête et non une entité — elle disparaît des labels de nœuds, si bien qu'elle n'apparaît jamais comme pastille de nœud dans l'interface graphe. [tool-verified: `provisa/api/admin/types.py:606-611`, `provisa/api/admin/db_queries.py:47-82`]

### Découverte de relations par l'IA

Déclenchez l'analyse des clés étrangères assistée par Claude via REST (REQ-167, REQ-018) :

```bash
curl -X POST http://localhost:8001/admin/discover/relationships \
  -H "Content-Type: application/json" \
  -d '{"scope": "domain", "domain_id": "sales"}'
```

Renvoie les candidats de clés étrangères classés par confiance. Accepter un candidat :

```bash
curl -X POST http://localhost:8001/admin/discover/candidates/{id}/accept \
  -H "Content-Type: application/json" \
  -d '{"name": "orders_to_customers"}'
```

### Introspection du schéma

Parcourez les tables publiées de toutes les sources (REQ-008) :

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

### Vérification des dépendances de colonnes (REQ-1484)

Avant d'enregistrer une édition de table qui renomme l'alias SQL d'une colonne ou supprime une
colonne, demandez ce qui la référence par ailleurs :

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

Renommer un alias casse tout artefact écrit contre le nom exposé — vues, vues matérialisées,
expressions de métriques, prédicats RLS, contrats de qualité des données. Supprimer une colonne
casse ceux-là ainsi que les artefacts qui stockent le `column_name` physique : relations, liaisons
de glossaire, affectations d'étiquettes. `breaksOn` dit lequel. La page Tables exécute cette
requête à l'enregistrement et en montre le résultat sous forme de boîte de dialogue consultative.
Voir [Traçabilité](lineage.md) pour ce que la requête couvre et ce qu'elle ne peut pas couvrir.

### Gestion des vues

Enregistrer une vue matérialisée (REQ-133, REQ-135) :

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

Déclencher un rafraîchissement manuel (REQ-135) :

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Enregistrement de sources graphe

Les sources Neo4j et SPARQL sont enregistrées via des endpoints REST (et non via l'API d'administration GraphQL) (REQ-295, REQ-297) :

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

Une fois enregistrées, les tables apparaissent dans le schéma GraphQL et sont interrogeables comme n'importe quelle autre source (REQ-016).

### Import Hasura / DDN (REQ-1483)

Convertissez un projet Hasura v2 ou Hasura DDN existant en configuration Provisa depuis l'interface ou l'API d'administration, sans que rien n'atterrisse avant votre approbation.

```http
POST /admin/import/hasura/preview
POST /admin/import/hasura/apply
```

**L'aperçu** convertit l'archive téléversée et renvoie le `config_yaml` proposé, une liste d'avertissements et un récapitulatif de ce qui a été trouvé (nombres de sources, domaines, tables, colonnes, rôles, relations et règles RLS). Rien n'est écrit dans la base du locataire. Corps de la requête :

```json
{
  "filename": "my-hasura-project.zip",
  "content_b64": "<base64-encoded archive>",
  "flavor": "auto",
  "domain_map": {"public": "sales"},
  "source_overrides": {}
}
```

`flavor` vaut `"auto"` (détecté d'après la structure de l'archive), `"hasura_v2"` ou `"ddn"`.

**L'application** prend le YAML que vous avez revu (et éventuellement édité) et le charge dans l'organisation active — le même chemin de rechargement à chaud que `PUT /admin/config`. Corps de la requête : `{"config_yaml": "<yaml string>"}`.

L'aperçu ne met jamais en cache côté serveur le YAML converti ; l'application prend le YAML que vous fournissez, si bien que ce qui est appliqué est exactement ce qui a été revu. [tool-verified: `provisa/api/admin/import_router.py`]

### Échange Apache Ossie (REQ-1316, REQ-1321)

Provisa interopère avec Apache Ossie (incubating) comme frontière d'import/export.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**L'export** (`GET /admin/ossie`) dérive le document YAML Ossie du modèle gouverné en direct à chaque requête — il n'est jamais mis en cache, il ne peut donc pas être périmé. La réponse est de type `text/yaml` avec un en-tête `Content-Disposition: attachment`. Les tables deviennent des objets `dataset`, les colonnes des objets `field`, et les relations correspondent aux objets `relationship` d'Ossie. (REQ-1321) [tool-verified: `provisa/api/admin/ossie_router.py:download_ossie`]

**L'import** (`POST /admin/ossie/import`) accepte un document Ossie YAML ou JSON (le format est détecté automatiquement). Il analyse le document et renvoie les enregistrements de tables et de relations proposés sous forme d'objet JSON — rien n'est enregistré. L'écran de revue de l'interface d'administration vous laisse accepter ou élaguer les propositions avant qu'aucune mutation ne se déclenche. (REQ-1316) [tool-verified: `provisa/api/admin/ossie_router.py:import_ossie`]

### Stockage objet (REQ-1046, REQ-1048, REQ-1049)

Lisez ou configurez le stockage de matérialisation de l'organisation :

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` indique quelle part de l'allocation de stockage de la plateforme l'organisation consomme. `PUT` enregistre le DSN de stockage propre à l'organisation (chiffré au repos ; jamais renvoyé par GET). Une fois défini, les matérialisations de l'organisation atterrissent dans son propre bucket et ne sont plus décomptées de l'allocation de la plateforme. Envoyer `storage_url: null` l'efface et ramène l'organisation sur le magasin de la plateforme. [tool-verified: `provisa/api/admin/org_storage_router.py`]

### Chiffrement de l'organisation (REQ-1574)

Définissez ou faites tourner la clé de chiffrement au repos de l'organisation :

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` renvoie l'empreinte de la clé, son identifiant et sa provenance — jamais le matériel de clé. `PUT` définit ou fait tourner la clé. Fournissez `key_b64` (32 octets bruts, encodés en base64) pour apporter votre propre clé, ou omettez-le pour que Provisa en génère une. Il n'existe pas de suppression : retirer la dernière clé rendrait illisible chaque charge utile qu'elle a enveloppée. [tool-verified: `provisa/api/admin/org_encryption_router.py`]

## GraphiQL

L'API d'administration embarque GraphiQL à `GET /admin/graphql` dans le navigateur (REQ-622). Utilisez-le pour explorer interactivement l'ensemble du schéma d'administration.

## Vues de gestion du domaine ops (REQ-1386)

Huit vues SQL sont amorcées dans le domaine `ops` intégré à chaque installation. [tool-verified: `provisa/api/startup_seed.py:225-331` `_seed_ops_domain`] Elles exposent le journal d'audit des requêtes comme des tables gouvernées — interrogeables via SQL (pgwire), GraphQL et Cypher sous les mêmes règles d'accès au domaine, de RLS et de masquage que n'importe quelle table métier.

`org_admin` est désigné intendant du domaine ops au moment de l'amorçage, si bien que le domaine n'apparaît jamais comme une lacune de gouvernance dans `stale_metadata`. [tool-verified: `startup_seed.py:326-331`]

| Vue | Ce à quoi elle répond |
| --- | --- |
| `usage_ranking` | Nombre de requêtes et utilisateurs distincts par table enregistrée ; les tables sans le moindre accès ressortent comme candidates à la mise hors service |
| `deprecated_usage` | Chaque accès à une table ou colonne portant l'étiquette `deprecated` — les consommateurs actifs qui bloquent un retrait sûr |
| `pii_access` | Chaque accès à une table ou colonne portant l'étiquette `pii` : qui l'a interrogée, sous quel rôle, sur quelle surface |
| `policy_denials` | Toutes les tentatives d'accès que la gouvernance a rejetées (HTTP 401/403) |
| `surface_mix` | Nombre de requêtes et utilisateurs distincts par jour et par surface de protocole (SQL, GraphQL, Cypher, gRPC, etc.) |
| `query_health` | Nombre d'erreurs par jour et latence moyenne / maximale par surface |
| `stale_metadata` | Tables et colonnes dépourvues de description ; domaines sans intendant |
| `join_hotspots` | Paires de tables les plus souvent co-interrogées — candidates à la matérialisation ou à la mise en cache |

Deux limites s'appliquent aujourd'hui. La granularité est au niveau de la table — le journal d'audit enregistre les `table_ids`, non les colonnes individuelles consultées. Le texte des requêtes est chiffré (REQ-689) et exclu de toutes les vues présentées ici ; il n'est accessible que par le chemin de déchiffrement administrateur autorisé. [tool-verified: `_meta_views.py:148-162` — comment notes `query_text_enc` exclusion]

Un rôle a besoin de l'accès au domaine `ops` pour que ces vues lui soient visibles. Accordez-le comme vous accordez l'accès à n'importe quel autre domaine.

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

La visionneuse de rapports se trouve à `/admin/reports`. Les rôles dépourvus de la capacité `observability` ne peuvent pas l'atteindre.

Le panneau de gauche liste chaque table enregistrée du domaine `ops`, triée par alias. [tool-verified: `ReportsTab.tsx:46-52` — filters `tables` to `domainId === "ops"`] Les huit vues de gestion amorcées y apparaissent automatiquement. Cliquez sur un rapport pour le charger dans la visionneuse de données gouvernées, à droite.

**Ajouter un rapport personnalisé.** Le bouton « Ajouter un rapport » ouvre une boîte de dialogue. Indiquez un nom, une description facultative et une instruction SELECT. L'enregistrement inscrit la vue comme table dérivée gouvernée dans le domaine `ops` — cataloguée, soumise au contrôle d'accès et interrogeable sur toutes les surfaces, aux côtés des vues amorcées. [tool-verified: `ReportsTab.tsx:70-96` — `registerTable` called with `sourceId: DERIVED_SOURCE_ID, domainId: "ops"`]

**Supprimer.** L'icône de corbeille n'apparaît que pour les rapports personnalisés. Les vues de gestion amorcées ne peuvent pas être supprimées depuis cette interface. [tool-verified: `ReportsTab.tsx:151` — `const custom = report.sourceId === DERIVED_SOURCE_ID` gates the delete button]

## Aperçu de table (REQ-1392)

Dépliez n'importe quelle ligne de table sur la page Tables. Le bouton **Aperçu** ouvre une fenêtre modale de 90 % de largeur avec les données gouvernées en direct de la table. [tool-verified: `TablePreviewModal.tsx:24` — `size="90%"`; `GovernedTableViewer.tsx` is the underlying viewer]

Les tables adossées à des API comportant des paramètres de chemin obligatoires bloquent l'aperçu tant que ces valeurs ne sont pas fournies. Un formulaire en ligne recueille chaque paramètre obligatoire avant l'exécution de la première requête ; les paramètres de requête facultatifs figurent dans le même formulaire. [tool-verified: `GovernedTableViewer.tsx:51-55, 153-155` — `requiredParamColumns` check ; "paramsRequired" message shown when `activeParams == null`]

## Visionneuse de données gouvernées (REQ-1391)

Le même composant de visionneuse alimente la fenêtre modale d'aperçu et la visionneuse de rapports. Son comportement est identique dans les deux contextes.

**Pagination côté serveur.** Chaque page est son propre `SELECT *` gouverné assorti de `LIMIT 101 OFFSET n`. Cent lignes apparaissent par page ; la 101ᵉ indique s'il en existe d'autres. Le jeu de données complet n'est jamais chargé dans le navigateur. [tool-verified: `nativeParams.ts:72` — `LIMIT ${pageSize + 1} OFFSET ${page * pageSize}`; `types.ts:74` — `PAGE_SIZE = 100`]

**Filtres et tris poussés vers la source.** Chaque en-tête de colonne comporte un champ de filtre. Les termes de filtre deviennent des prédicats `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')` ; les clics de tri produisent des clauses `ORDER BY`. Les deux partent vers la base de données — un filtre sur une table d'un milliard de lignes balaie la source, non la page de 100 lignes qui est devant vous. [tool-verified: `nativeParams.ts:53-70`]

**Regroupement multiniveau.** L'icône Calques d'un en-tête de colonne bascule cette colonne dans le regroupement. Les colonnes de groupe ouvrent l'`ORDER BY` pour que les membres d'un groupe atterrissent sur la même page que leur en-tête, par-delà les frontières de page. Les colonnes de clé primaire sont ajoutées en fin de liste comme départage stable. [tool-verified: `nativeParams.ts:61-70` — group columns first, then explicit sorts, then PKs] Les lignes d'en-tête de groupe sont repliables ; les replier masque les membres sans émettre de nouvelle requête. [tool-verified: `useResultsGrid.ts:150-171` — `collapsedGroups` set gates the `build()` recursion]

**Choix persistants.** Les réglages de filtre, de tri et de regroupement sont persistés dans `localStorage` sous `provisa.grid.table:<domain>.<table>` et restaurés à la visite suivante. [tool-verified: `useResultsGrid.ts:95-98`, `GovernedTableViewer.tsx:66`]

**Export.** Téléchargez la page courante en CSV, ou copiez-la dans le presse-papiers en texte séparé par des tabulations. L'export ne couvre que la page visible. [tool-verified: `useResultsGrid.ts:247-274` — both handlers iterate `displayRows`, which in server-paged mode is the current page]
