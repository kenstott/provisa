# API d'administration {: #admin-api }

L'API d'administration est un endpoint GraphQL Strawberry à `POST /admin/graphql`. Elle exige un rôle superuser ou admin et est distincte de l'endpoint GraphQL de données.

## Authentification {: #authentication }

Transmettez vos identifiants dans l'en-tête `Authorization` en utilisant le fournisseur d'authentification Provisa standard :

```yaml
Authorization: Bearer <token>
```

L'accès administrateur est régi par la capacité `admin` affectée à un rôle.

### Jetons d'accès personnels {: #personal-access-tokens }

Un jeton d'accès personnel est accepté partout où un jeton bearer l'est, y compris sur ce point de terminaison. Vous émettez et révoquez les vôtres, car un jeton est l'identifiant propre de son détenteur. C'est pourquoi il se trouve dans votre profil de l'interface d'administration, à côté de quitter une organisation et de supprimer votre compte, et non sous une page d'administration. Un administrateur n'émet jamais de jeton au nom de quelqu'un d'autre.

| Route | Effet |
| ------- | -------- |
| `POST /auth/tokens` | Frappe un jeton pour l'appelant. Corps : `name`, en option `role_id`, `scopes`, `expires_in_days` (1–366). La réponse est le seul endroit où le secret apparaîtra jamais |
| `GET /auth/tokens` | Les jetons actifs de l'appelant dans cette organisation — préfixe d'affichage, nom, horodatages de cycle de vie et l'empreinte qui identifie un jeton en vue de sa révocation. Jamais un identifiant utilisable |
| `DELETE /auth/tokens/{token_hash}` | Révoque l'un des jetons de l'appelant. 404 lorsqu'il ne lui appartient pas ou a déjà été révoqué |

Omettre `role_id` laisse le jeton se résoudre vers le rôle que détient son propriétaire ; en nommer un restreint le jeton en deçà de son propriétaire. La révocation a lieu aussi implicitement : retirer l'appartenance d'un utilisateur à une organisation révoque ses jetons pour celle-ci. Voir [Modèle de sécurité](security.md#personal-access-tokens) pour l'identifiant lui-même.

## Capacités {: #capabilities }

### Gestion de la configuration {: #config-management }

Téléchargez la configuration en cours d'exécution :

```http
GET /admin/config
```

Renvoie le `config.yaml` complet sous forme de fichier YAML. Téléversez une nouvelle configuration :

```http
PUT /admin/config
```

Provisa valide le YAML, recharge les catalogues et régénère les schémas. Aucun redémarrage requis.

### Paramètres d'exécution {: #runtime-settings }

Lisez et écrivez les paramètres de plateforme à l'exécution sans éditer le fichier de configuration :

```http
GET  /admin/settings
PUT  /admin/settings
```

La surface des paramètres couvre la redirection des grands résultats, l'échantillonnage et la limite de lignes par défaut, le TTL du cache de réponses, la convention de nommage, le suivi automatique des clés étrangères de relations, le DSN du magasin de matérialisation, la mémoire du moteur de fédération (`jvm_heap_gb`, `query_max_memory`, `query_max_memory_per_node`, `query_max_total_memory`, `fault_tolerant_execution`, `fault_tolerant_task_memory`, `exchange_spool_dir`) ainsi que l'ensemble de la surface de réglage du pipeline de traçage OpenTelemetry. Les limites de traversée GraphQL distante et les paramètres de niveau tiède / cache de lecture sont également exposés.

La carte **Serveurs des connecteurs** nomme, pour chaque connecteur avec un serveur intégré (File Crawler, SharePoint, Salesforce, ServiceNow, Cloud Inventory, Splunk, AskAmerica), un dossier contenant un paquet de serveur compilé pour cet hôte (`connectors.<connector>.bundle_dir`, variable d'environnement `PROVISA_PGWIRE_BUNDLE_<CONNECTOR>`). Vide, le serveur du connecteur est récupéré depuis la version épinglée de `kenstott/calcite`. Défini, le dossier est utilisé tel quel : son lanceur doit se trouver à `bin/pgwire-<connector>`, et un dossier sans lui empêche le démarrage du serveur du connecteur avec une erreur nommant le paramètre ; la version n'est jamais récupérée à sa place. Le paramètre est lu à chaque démarrage du serveur d'un connecteur.

La posture de sécurité, `security.mode` (`standard` | `high`), s'applique au redémarrage :

```http
GET  /admin/security
PUT  /admin/security
```

Les affectations de modèles d'IA, le registre des modèles d'embedding et de vecteurs et la limite de débit NL prennent effet à la requête suivante, sans redémarrage :

```http
GET  /admin/ai-models
PUT  /admin/ai-models
```

L'onglet de chiffrement de l'administration dérive sa liste de fournisseurs en direct du registre de chiffrement ; les fournisseurs indisponibles y apparaissent mais ne sont pas sélectionnables.

`GET`/`HEAD /health` et `GET /setup/status` n'exigent jamais d'authentification. Ils échappent à l'exigence `Authorization: Bearer` même quand un fournisseur d'authentification est configuré.

### Moteur de fédération {: #federation-engine }

Lisez ou changez le moteur qu'utilise le déploiement :

```http
GET  /admin/federation-engine
PUT  /admin/federation-engine
```

`GET` renvoie la clé du moteur actif et les champs de configuration dont il a besoin. `PUT` accepte un corps comportant `engine` (la clé) et tout champ propre au moteur ; la sélection est persistée dans la configuration de la plateforme et prend effet au prochain redémarrage du service.

### Éditeur de relations {: #relationship-editor }

Lister les relations :

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

Créer une relation. Un identifiant qui existe déjà est refusé ; une relation se modifie avec `updateRelationship`, qui prend la même entrée :

```graphql
mutation {
  createRelationship(input: {
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

Déclarer une relation adossée à une jonction :

```graphql
mutation {
  createRelationship(input: {
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

Vous déclarez une table associative comme une arête ; Provisa n'en découvre jamais. `viaTable` nomme une table enregistrée. Ses deux colonnes clés portent l'arête, et chaque autre colonne devient un attribut de la relation, filtrable comme n'importe quel champ. `viaTypeColumn` / `viaTypeValue` découpent une table de jonction en plusieurs types d'arêtes. Par exemple, les lignes de `pet_companions` dont `companion_type` vaut `bonded pair`, `littermate` ou `shares enclosure` donnent trois relations distinctes sur la même paire de tables.

`viaLabelSource` indique d'où vient le nom exposé, et Cypher reçoit chaque forme en majuscules avec tirets bas. `column` utilise `viaTypeValue` (`BONDED_PAIR`), `table` le nom propre de la table de jonction (`PET_COMPANIONS`) et `fixed` l'`alias` déclaré. Une table de jonction déclarée ainsi est une arête. Provisa la retire des étiquettes de nœud ; elle n'apparaît donc jamais comme pastille de nœud dans l'interface du graphe.

### Découverte de relations par l'IA {: #ai-relationship-discovery }

Demandez à Claude des candidats de clés étrangères via REST :

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

### Introspection du schéma {: #schema-introspection }

Parcourez les tables publiées de toutes les sources :

```graphql
query {
  tables {
    id
    sourceId
    columns {
      columnName
      unmaskedTo
    }
    writeGrants {
      role
      operation
      columns
      rows
      check
      presets { column source name value }
    }
  }
}
```

### Vérification des dépendances de colonnes {: #column-dependency-check }

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

Renommer un alias casse chaque artefact écrit contre le nom exposé : vues, MV, expressions de métriques, prédicats RLS et contrats DQ. Supprimer une colonne casse aussi ceux-là, plus les artefacts qui stockent le `column_name` physique, c'est-à-dire les relations, les liaisons du glossaire et les affectations de tags. `breaksOn` dit lequel. La page Tables lance ce contrôle à l'enregistrement et affiche le résultat dans une boîte de dialogue d'avertissement. [Traçabilité](lineage.md) explique ce que couvre la requête et ce qu'elle ne peut pas couvrir.

### Gestion des vues {: #view-management }

Enregistrer une vue matérialisée :

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

Déclencher un rafraîchissement manuel :

```graphql
mutation {
  refreshMv(mvId: "orders-with-customers") {
    success
  }
}
```

### Enregistrement de sources graphe {: #graph-source-registration }

Les sources Neo4j et SPARQL sont enregistrées via des endpoints REST (et non via l'API d'administration GraphQL) :

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

Une fois enregistrées, les tables apparaissent dans le schéma GraphQL et sont interrogeables comme n'importe quelle autre source.

### Import Hasura / DDN {: #hasura-ddn-import }

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

**L'application** charge le YAML que vous avez relu, et modifié si vous l'avez voulu, dans l'organisation qui agit. Elle suit le même chemin de rechargement à chaud que `PUT /admin/config`. Corps de la requête : `{"config_yaml": "<yaml string>"}`.

L'aperçu ne met jamais en cache côté serveur le YAML converti ; l'application prend le YAML que vous fournissez, si bien que ce qui est appliqué est exactement ce qui a été revu.

### Échange Apache Ossie {: #apache-ossie-interchange }

Provisa interopère avec Apache Ossie (incubating) comme frontière d'import/export.

```http
GET  /admin/ossie
POST /admin/ossie/import
```

**Export** (`GET /admin/ossie`) construit le document YAML Ossie à partir du modèle gouverné vivant à chaque requête. Rien n'est mis en cache, le document n'est donc jamais périmé. La réponse est `text/yaml` avec un en-tête `Content-Disposition: attachment`. Le document est un modèle avec `version`, `name`, `datasets`, `relationships` et `metrics` à sa racine. Les tables deviennent des objets `dataset`, les colonnes des objets `field`, et les relations correspondent à des objets Ossie `relationship`. Un modèle sans table est refusé avec un `422`.

**Import** (`POST /admin/ossie/import`) accepte un document Ossie en YAML ou JSON et détecte le format lui-même. Il analyse le document et renvoie, sous forme d'objet JSON, les enregistrements proposés de tables, de relations et de métriques. Rien n'est enregistré. Il ne lit que la forme plate du document, et un document qui enveloppe son modèle dans `semantic_model` est refusé nommément. Dans l'interface d'administration, l'écran de revue vous permet d'accepter ou d'élaguer les propositions avant tout changement.

### Stockage objet {: #object-storage }

Lisez ou configurez le stockage de matérialisation de l'organisation :

```http
GET  /admin/org-storage
PUT  /admin/org-storage
```

`GET` indique quelle part de l'allocation de stockage de la plateforme l'organisation consomme. `PUT` enregistre le DSN de stockage propre à l'organisation (chiffré au repos ; jamais renvoyé par GET). Une fois défini, les matérialisations de l'organisation atterrissent dans son propre bucket et ne sont plus décomptées de l'allocation de la plateforme. Envoyer `storage_url: null` l'efface et ramène l'organisation sur le magasin de la plateforme.

### Chiffrement de l'organisation {: #org-encryption }

Définissez ou faites tourner la clé de chiffrement au repos de l'organisation :

```http
GET  /admin/org-encryption
PUT  /admin/org-encryption
```

`GET` renvoie l'empreinte, l'id et la provenance de la clé, jamais le matériel de clé. `PUT` définit ou fait tourner la clé. Fournissez `key_b64` (32 octets bruts, encodés en base64) pour apporter votre propre clé, ou omettez-le et Provisa en génère une. Il n'y a pas de suppression : retirer la dernière clé rendrait illisible chaque charge qu'elle a enveloppée.

## GraphiQL

L'API d'administration embarque GraphiQL à `GET /admin/graphql` dans le navigateur. Utilisez-le pour parcourir l'ensemble du schéma d'administration et essayer des appels.

## Vues de gestion du domaine ops {: #ops-domain-management-views }

Chaque installation crée huit vues SQL dans le domaine intégré `ops`. Elles exposent le journal d'audit des requêtes sous forme de tables gouvernées. Vous les interrogez en SQL (pgwire), GraphQL et Cypher, avec les mêmes règles d'accès au domaine, de RLS et de masquage que n'importe quelle table métier.

`org_admin` est désigné intendant du domaine ops au moment de l'amorçage, si bien que le domaine n'apparaît jamais comme une lacune de gouvernance dans `stale_metadata`.

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

La granularité s'arrête à la table. Le journal d'audit enregistre `table_ids`, pas les colonnes précises qu'une requête a lues. Le texte des requêtes est chiffré et absent de toutes ces vues ; seul le chemin de déchiffrement autorisé de l'administration peut le lire.

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

Comme les vues sont des tables gouvernées, vous pouvez écrire les mêmes requêtes en GraphQL ou en Cypher.

## Visionneuse de rapports {: #reports-viewer }

La visionneuse de rapports se trouve à `/admin/reports`. Les rôles dépourvus de la capacité `observability` ne peuvent pas l'atteindre.

Le panneau de gauche liste chaque table enregistrée du domaine `ops`, triée par alias. Les huit vues de gestion amorcées y apparaissent automatiquement. Cliquez sur un rapport pour le charger dans la visionneuse de données gouvernées, à droite.

**Ajouter un rapport personnalisé.** Le bouton « Add report » ouvre une boîte de dialogue où vous donnez un nom, une description facultative et une instruction SELECT. L'enregistrement inscrit la vue comme table dérivée gouvernée dans le domaine `ops`. Elle est cataloguée, soumise au contrôle d'accès et interrogeable depuis chaque surface, à côté des vues prédéfinies.

**Supprimer.** L'icône de corbeille n'apparaît que pour les rapports personnalisés. Les vues de gestion amorcées ne peuvent pas être supprimées depuis cette interface.

## Aperçu de table {: #table-preview }

Dépliez n'importe quelle ligne de table sur la page Tables. Le bouton **Aperçu** ouvre une fenêtre modale de 90 % de largeur avec les données gouvernées en direct de la table.

Les tables adossées à des API comportant des paramètres de chemin obligatoires bloquent l'aperçu tant que ces valeurs ne sont pas fournies. Un formulaire en ligne recueille chaque paramètre obligatoire avant l'exécution de la première requête ; les paramètres de requête facultatifs figurent dans le même formulaire.

## Visionneuse de données gouvernées {: #governed-data-viewer }

Le même composant de visionneuse alimente la fenêtre modale d'aperçu et la visionneuse de rapports. Son comportement est identique dans les deux contextes.

**Pagination côté serveur.** Chaque page est son propre `SELECT *` gouverné assorti de `LIMIT 101 OFFSET n`. Cent lignes apparaissent par page ; la 101ᵉ indique s'il en existe d'autres. Le jeu de données complet n'est jamais chargé dans le navigateur.

**Filtres et tris poussés vers la source.** Chaque en-tête de colonne a un champ de filtre. Un terme de filtre devient un prédicat `WHERE LOWER(CAST(col AS VARCHAR)) LIKE LOWER('%term%')`, et un clic de tri produit une clause `ORDER BY`. Les deux s'exécutent dans la base de données. Un filtre sur une table d'un milliard de lignes parcourt la source, et sa portée ne se limite jamais à la page de 100 lignes sous vos yeux.

**Regroupement multiniveau.** L'icône Calques d'un en-tête de colonne bascule cette colonne dans le regroupement. Les colonnes de groupe ouvrent l'`ORDER BY` pour que les membres d'un groupe atterrissent sur la même page que leur en-tête, par-delà les frontières de page. Les colonnes de clé primaire sont ajoutées en fin de liste comme départage stable. Les lignes d'en-tête de groupe sont repliables ; les replier masque les membres sans émettre de nouvelle requête.

**Choix persistants.** Les réglages de filtre, de tri et de regroupement sont persistés dans `localStorage` sous `provisa.grid.table:<domain>.<table>` et restaurés à la visite suivante.

**Export.** Téléchargez la page courante en CSV, ou copiez-la dans le presse-papiers en texte séparé par des tabulations. L'export ne couvre que la page visible.
