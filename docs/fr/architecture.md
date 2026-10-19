# Architecture de Provisa {: #provisa-architecture }

## Vue d'ensemble {: #overview }

Provisa est une plateforme de virtualisation de données pilotée par configuration : fédération, métriques, ontologie et gouvernance à partir d'un seul modèle, pour les petites équipes comme pour les grandes entreprises. Elle fournit une API unifiée sur des sources de données hétérogènes avec gouvernance, sécurité, et optimisation de performance. Les clients interrogent via SQL, GraphQL, ou Cypher ; les trois sont des interfaces de premier ordre avec une gouvernance identique appliquée.

Le modèle est le seul endroit où du sens est ajouté. Pour l'enrichir, vous enregistrez de nouvelles sources de données ou de nouveaux agrégats dans la couche de virtualisation des données. Cela garde une séparation nette, car rien ne peut ajouter de sémantique en dehors de la plateforme, et c'est ce qui rend possible une vraie gouvernance des données. Le compilateur l'impose : le catalogue des relations approuvées fait foi, quel que soit le langage de requête.

Provisa est conçu pour être hautement performant pour les besoins opérationnels et hautement scalable pour les besoins analytiques d'entreprise. Une plateforme unique sert les deux sans sacrifier la vitesse ou la scalabilité.

```text
Config YAML → PG Metadata → Federation Catalogs
                               ↓
         Federation engine metadata → Schema Generator → SDL / SQL catalog / Cypher labels / gRPC proto (per role)
                                     ↓
                     Query → Parser → SQL Compiler → Transpiler
                                     ↓
                             Router (Smart Dispatch)
                         /           |            \
                    Federation  Direct PG      Direct MySQL/etc.
                         \           |            /
                              Executor Pool
                                     ↓
                         ┌───── Inline ─────┐     ┌──── Redirect ────┐
                         │  JSON (HTTP)     │     │  CTAS → S3       │
                         │  Arrow (Flight)  │     │  (Parquet, ORC)  │
                         │  Protobuf (gRPC) │     │  Provisa → S3    │
                         └─────────────────-┘     │  (JSON, CSV, …)  │
                                                  └─────────────────-┘
```

## Interfaces de requête {: #query-interfaces }

Chaque interface est un transport distinct, et les quatre appliquent le même pipeline de sécurité (RLS, masquage, échantillonnage, contrôles de rôle). Les clients ne parlent jamais directement au moteur de fédération. Le langage de requête (SQL, GraphQL ou Cypher) est indépendant du transport, et plusieurs langages peuvent arriver par le même transport.

| Port | Transport | Langages de requête acceptés | Cas d'usage |
| ------ | ----------- | -------------------------- | ---------- |
| 8001 | HTTP | GraphQL, SQL, Cypher | Clients web, outils BI, curl, consommateurs REST |
| 8815 | Arrow Flight (gRPC) | SQL (via Arrow Flight SQL) | Outils de données (Pandas, DuckDB, Spark, ADBC) |
| 50051 | Protobuf gRPC | RPC proto générés par rôle | Service-à-service avec contrats typés |
| configurable¹ | Protocole de fil PostgreSQL (pgwire) | SQL | psql, DBeaver, SQLAlchemy, tout client compatible PG |

¹ Définissez `PROVISA_PGWIRE_PORT` (ex. 5433). Désactivé quand non défini ou `0`.

### HTTP (port 8001)

Plusieurs endpoints sous le même port, distingués par chemin :

| Chemin | Langage | Notes |
| ------ | ---------- | ------- |
| `POST /data/graphql` | GraphQL | Lectures et mutations ; hash APQ accepté via `extensions.persistedQuery` |
| `POST /data/sql` | SQL | Lecture seule ; aucune porte de capacité — gouverné par visibilité d'objet + RLS + masquage |
| `POST /data/query` | Cypher | Lecture seule ; rôle standard |
| `GET /data/nl` | Langage naturel | Traduit vers SQL/GraphQL/Cypher selon le type de source |
| `GET /data/subscribe/{table}` | GraphQL | Flux d'abonnement SSE |
| `GET /neo4j/...` | Cypher (compat Neo4j) | Cale de compatibilité API HTTP Neo4j |
| `POST /admin/graphql` | GraphQL | API admin (rôle superuser/admin requis) |

Tous les chemins retournent du JSON par défaut. `Accept: text/csv`, `application/vnd.apache.parquet`, `application/vnd.apache.arrow.stream`, et `application/octet-stream` (binaire brut) sont pris en charge via la négociation de contenu. Les résultats dépassant le seuil de taille configuré sont automatiquement redirigés vers une URL S3 signée.

### Arrow Flight (port 8815)

Transport columnaire Arrow natif sur gRPC. Les clients envoient un ticket JSON :

```json
{"query": "SELECT name, email FROM customers", "role": "analyst"}
```

et reçoivent des RecordBatches Arrow streamés paresseusement. Quand le proxy Flight SQL Zaychik est disponible, les données circulent comme un flux de lots d'enregistrements Arrow de bout en bout :

```text
Client ←(Arrow batches)← Provisa Flight Server ←(Arrow batches)← Zaychik ←(JDBC)← Federation Engine
```

Le résultat complet n'est jamais conservé dans la mémoire de Provisa : les lots sont transmis à mesure qu'ils arrivent. Arrow Flight est donc un chemin sans limite, adapté aux résultats de toute taille.

### Protobuf gRPC (port 50051)

`.proto` auto-généré depuis le schéma de données, généré par rôle. Requêtes en streaming (un message par ligne), mutations unaires. Réflexion serveur activée. Rôle via la clé de métadonnées `x-provisa-role`. La réflexion décrit à chaque appelant le service de son propre rôle.

### Protocole de fil PostgreSQL / pgwire (port configurable) {: #postgresql-wire-protocol-pgwire-configurable-port }

Implémente le protocole de fil frontend/backend PostgreSQL en utilisant la bibliothèque `buenavista`. Tout client compatible PostgreSQL — `psql`, DBeaver, SQLAlchemy avec `psycopg2`, JDBC — peut se connecter sans modification. Accepte uniquement SQL. Le pipeline de gouvernance complet (RLS, masquage, permissions de domaine) s'applique identiquement aux connexions pgwire. Activé en définissant `PROVISA_PGWIRE_PORT` à un port non nul.

## Pipeline de requête {: #request-pipeline }

Trois langages de requête sont acceptés, et tous trois convergent vers la gouvernance après leurs propres étapes d'analyse et de compilation. Tous trois peuvent aussi écrire : les mutations GraphQL, les `INSERT`, `UPDATE`, `DELETE`, `MERGE` et `TRUNCATE` SQL, et les `CREATE`, `SET` et `DELETE` Cypher se traduisent en SQL et passent par la même admission d'écriture. L'interrogation elle-même n'a aucune barrière de capacité. Toute identité authentifiée peut interroger dans n'importe quel langage, et les données ne sont gouvernées que par la visibilité des objets, la RLS et le masquage.

| Interface | Lectures | Écritures | Porte de requête |
| --- | --- | --- | --- |
| GraphQL (`/data/graphql`) | Oui | Oui (mutations) | Aucune — gouvernance de couche de données uniquement |
| SQL (`/data/sql`) | Oui | Non | Aucune — gouvernance de couche de données uniquement |
| Cypher (`/data/query`) | Oui | Non | Aucune — gouvernance de couche de données uniquement |

```mermaid
flowchart TD
    A[GraphQL Request] --> B[Auth / Role Resolution]
    A2[SQL Request] --> B
    A3[Cypher Request] --> B
    B --> E[APQ Hash Check]
    E --> F[Parse & Validate]
    F --> G[Extract Directives / Hints]
    G --> H{Cache Hit?}
    H -- yes --> R
    H -- no --> I{Input Type}
    I -- GraphQL --> I1[Compile → Semantic SQL]
    I -- SQL --> I2[Parse & Validate SQL\nApply Namespace / Source Binding]
    I -- Cypher --> I3[Translate Cypher → SQL\nResolve Node / Rel Mappings]
    I1 --> J[Governance: RLS + Masking + Visibility + Sampling]
    I2 --> J
    I3 --> J
    J --> K[MV Rewrite]
    K --> L{Route}
    L -- Direct --> M[Transpile → Source Dialect\nExecute via Driver]
    L -- Federation --> N[Transpile → Federation SQL\nInject Session Hints\nExecute via Federation Engine / Flight]
    L -- Materialize --> O[Fetch from REST / GraphQL / gRPC\nMaterialize → S3 Parquet\nPost-filter via Federation Engine]
    L -- Mutation --> P[RLS Injection\nTranspile → Source Dialect\nExecute via Driver\nInvalidate Cache + MV\nEmit Change Event]
    M --> Q{Redirect?}
    N --> Q
    O --> Q
    Q -- yes --> S[Upload to S3\nReturn Signed URL]
    Q -- no --> R[Serialize: JSON / CSV / Parquet / Arrow]
    R --> T[Store in Cache]
    T --> U[Return to Client]
    P --> U
```

**Décisions de routage :**

| Route | Quand |
| --- | --- |
| **Cache** | Succès du cache de résultat — évalué en premier, sert le résultat stocké sans exécution |
| **Cheap-count** | Requête en forme de `count(*)` sur une source non matérialisée qui expose un compte natif exact — routée vers l'appel de compte natif au lieu de matérialiser pour compter |
| **Direct** | Source unique + a un pilote natif + a un connecteur de fédération |
| **Fédération** | Fédération multi-source, ou la source a un connecteur mais pas de pilote |
| **Matérialiser** | La source n'a pas de connecteur de fédération — récupérer et mettre en cache vers S3/PG d'abord |
| **Mutation** | Une écriture : le pilote propre de la source quand elle en a un ; sinon le moteur, via un connecteur qui écrit la source |

Le routage consomme la sortie de l'étape d'optimisation post-gouvernance, jamais le SQL gouverné pré-optimisation. La gouvernance peut AJOUTER des sources (prédicats de sous-requête RLS) ; l'étape d'optimisation peut les RETIRER (inlining VALUES-CTE de table chaude, réécritures de cache API, élagage de branche union). Une requête fédérée qui s'effondre en une seule source live après inlining est donc re-routée comme directe.

### Requêtes multi-racine {: #multi-root-queries }

Les requêtes GraphQL avec plusieurs champs racine (ex. `{ orders { id } customers { name } }`) sont compilées en requêtes SQL séparées et exécutées indépendamment. Les requêtes SQL et Cypher sont mono-racine par définition. Les résultats sont fusionnés dans une seule réponse :

- Les champs sous le seuil de redirection sont retournés en ligne dans `data`
- Les champs au-dessus du seuil sont redirigés, avec des entrées par champ dans `redirects`
- Les formats binaires (Parquet, Arrow) ne sont pris en charge que pour les requêtes mono-racine

## Chemins d'exécution de fédération {: #federation-execution-paths }

| Chemin | Transport | Via | Quand utilisé |
| ------ | ----------- | ----- | ----------- |
| REST | client du moteur de fédération (HTTP :8080) | Requête directe | Par défaut, toujours disponible |
| Flight SQL | `adbc-driver-flightsql` (gRPC :8480) | Proxy Zaychik → JDBC | Quand Zaychik tourne |
| CTAS | client du moteur de fédération (HTTP :8080) | Écriture directe, Iceberg vers S3 | Redirection Parquet/ORC |

### Proxy Arrow Flight SQL Zaychik {: #zaychik-arrow-flight-sql-proxy }

Le moteur de fédération ne prend pas nativement en charge le protocole Arrow Flight SQL. [Zaychik](https://github.com/Raiffeisen-DGTL/zaychik-trino-proxy) est un proxy Java qui implémente l'interface gRPC Arrow Flight SQL, traduit les requêtes en requêtes JDBC, et streame les résultats en retour comme lots d'enregistrements Arrow.

```text
ADBC client → gRPC :8480 → Zaychik → JDBC :8080 → Federation Engine → results → Arrow batches → client
```

Le serveur Flight de Provisa (port 8815) se connecte à Zaychik comme client ADBC, permettant le streaming Arrow de bout en bout sans matérialiser les résultats.

### Catalogue de résultats Iceberg {: #iceberg-results-catalog }

La redirection CTAS utilise un connecteur Iceberg (catalogue `results`) adossé à un catalogue JDBC sur l'instance PostgreSQL existante. Iceberg écrit les fichiers Parquet/ORC directement vers MinIO/S3 via le système de fichiers S3 natif (`fs.native-s3.enabled=true`).

## Moteurs de fédération {: #federation-engines }

Provisa choisit le moteur de fédération au démarrage à partir de la variable d'environnement `PROVISA_ENGINE`, de la configuration enregistrée depuis l'interface d'administration ou de la valeur par défaut. Si rien n'est défini, le défaut est DuckDB, entièrement dans le processus et sans service externe. Voir le fonctionnement de la sélection dans [Configuration](configuration.md#federation-engine).

Chaque moteur est une instance `FederationEngine` définie dans `provisa/federation/engine.py`. L'instance possède une collection de connecteurs qui détermine quels types de source le moteur peut lire en direct (ATTACH) versus lesquels doivent d'abord atterrir dans le magasin de matérialisation du moteur.

### Classes de pilote {: #driver-classes }

| Classe | Signification | Exemples |
| ------- | --------- | --------- |
| `BROAD` | Atteint de nombreux types de source externe via des connecteurs natifs | Trino |
| `PARTIAL` | Atteint un sous-ensemble (relationnel, fichiers, objet cloud/lac) plus fait atterrir tout le reste | DuckDB, PostgreSQL, ClickHouse, Databricks, Snowflake, BigQuery, Fabric, Synapse |
| `SELF_ONLY` | Atteint uniquement son propre magasin ; toute autre source atterrit | SQLAlchemy |

### Moteurs disponibles {: #available-engines }

| Clé moteur | Dialecte | MPP | Mécanisme de lien externe | Auth |
| ----------- | --------- | ----- | ------------------------ | ------ |
| `trino` / `trino-byo` | Trino SQL | Oui | Catalogues Trino (jeu de connecteurs large) | Identifiants JDBC |
| `pg` | PostgreSQL | Non | FDW / pg_duckdb | Identifiants PostgreSQL |
| `duckdb` | DuckDB | Non | ATTACH natif d'extension | Aucune (en processus) |
| `clickhouse` / `clickhouse-server` | ClickHouse | Oui (fragments) | Moteurs de table S3 / IcebergS3 / DeltaLake | Identifiants ClickHouse |
| `snowflake` | Snowflake | Oui | Stage externe + table externe | `PROVISA_ENGINE_URL` |
| `databricks` | Databricks SQL | Oui | Tables externes Unity Catalog via REST | Jeton bearer (`http_path` dans `federation_hints`) |
| `bigquery` | BigQuery | Oui (Dremel) | Tables externes BigQuery / BigLake | Clé de compte de service `GOOGLE_APPLICATION_CREDENTIALS` |
| `fabric` | T-SQL | Oui | Raccourcis OneLake → OPENROWSET | Azure AD (`az login` / identité managée) |
| `synapse` | T-SQL | Oui | ADLS OPENROWSET / tables externes | Azure AD |
| `sqlalchemy` | Tout dialecte SQLAlchemy | Non | Aucun (atterrissage uniquement) | Identifiants par dialecte |

### Défaut zéro-config : DuckDB {: #zero-config-default-duckdb }

Quand `PROVISA_ENGINE` n'est pas défini, Provisa utilise le moteur DuckDB entièrement embarqué en processus. Le magasin de matérialisation de DuckDB est un fichier DuckDB embarqué à `$PROVISA_DATA_DIR/materialize.duckdb` (par défaut `~/.provisa/materialize.duckdb`). Aucune base de données ou service externe n'est requis.

DuckDB n'autorise qu'un seul écrivain par fichier ; `store_connection.py` écrit donc dans le magasin embarqué via la propre connexion du moteur et n'ouvre jamais une seconde connexion indépendante. C'est le seul cas où le moteur et le magasin de matérialisation partagent un descripteur de fichier par conception.

### Transport de lecture Arrow-natif {: #arrow-native-read-transport }

ClickHouse, DuckDB, Snowflake, Databricks, BigQuery, Fabric, et Synapse annoncent tous `EngineCapability.ARROW` et `EngineCapability.ARROW_STREAM`. Les requêtes contre ces moteurs retournent directement des RecordBatches Arrow — le chemin de sérialisation en lignes est entièrement contourné. Le serveur Flight streame ces lots vers les clients sans matérialiser le résultat complet dans la mémoire processus de Provisa. Pour Trino, le streaming Arrow s'appuie sur le proxy Zaychik ; pour les moteurs d'entrepôt, la propre API Arrow-native du moteur (Cloud Fetch pour Databricks, Storage Read API pour BigQuery, `fetch_arrow_table` pour DuckDB et Snowflake) alimente le flux Flight.

### Liens de données externes (ATTACH) {: #external-data-links-attach }

Chaque moteur d'entrepôt peut scanner les données objet cloud/lac en place sans faire atterrir une copie. Les fichiers Parquet, CSV, Iceberg, et Delta Lake sur S3, GCS, ou OneLake s'attachent directement au moteur comme s'ils étaient des tables natives. La stratégie — ATTACH (scanner en place) ou LAND (copier dans le magasin) — est déterminée par le `Mechanism` déclaré du connecteur ; aucune ramification spécifique au moteur n'existe dans le planificateur. Un connecteur `Mechanism.ATTACH_R` déclenche un scan zéro-copie ; un connecteur `Mechanism.DIRECT` ou absent déclenche un atterrissage.

Attach auto-provisionne tous les prérequis au moment de l'attachement :

| Moteur | Formats objet/lac | Mécanisme | Auto-provisionnement |
| -------- | ------------------- | ---------- | ---------------------------------- |
| Databricks | parquet, csv, iceberg, delta_lake | Table externe UC (`ATTACH_R`) | REST installe un identifiant de stockage Unity Catalog + un emplacement externe, puis `CREATE TABLE … USING <format> LOCATION …` — vérifié en direct sur Cloudflare R2 |
| BigQuery | parquet, csv, json, iceberg, delta_lake | Table externe BigQuery / BigLake (`ATTACH_R`) | `CREATE OR REPLACE EXTERNAL TABLE … OPTIONS(format=…, uris=[…])` — vérifié en direct |
| ClickHouse | csv, parquet, iceberg, delta_lake | Moteur de table S3 / IcebergS3 / DeltaLake (`ATTACH_R`) | Sonde de validation exécutée au moment de l'attachement — vérifié en direct sur Cloudflare R2 |
| Fabric | parquet, csv, iceberg, delta_lake | Raccourci OneLake → OPENROWSET (`ATTACH_R`) | REST crée une connexion `AmazonS3Compatible` + un lakehouse + un raccourci ; retourne le chemin OneLake `BULK` — vérifié en direct en lisant R2 à travers Fabric |
| Snowflake | parquet, csv, json, iceberg, delta_lake | Stage externe + table externe (`ATTACH_R`) | `CREATE STAGE … URL=… CREDENTIALS=…`, puis `CREATE OR REPLACE EXTERNAL TABLE … LOCATION=@stage FILE_FORMAT=(TYPE=…)` — implémenté ; non testé en direct (aucun compte disponible) |

Les identifiants pour le stockage cloud voyagent dans le `federation_hints` de la source (voir [Sources](sources.md#warehouses-as-named-sources)). Tout type de source qui ne peut pas s'ATTACH atterrit d'abord dans le magasin de matérialisation du moteur.

### Écritures de matérialisation columnaire {: #columnar-materialization-writes }

`Connection.bulk_copy` dans `provisa/core/database.py` choisit le chemin d'ingestion en masse le plus rapide par dialecte de magasin : `COPY` binaire (`copy_records_to_table` d'asyncpg) pour les magasins PostgreSQL, et une seule instruction préparée `executemany` pour tous les autres magasins relationnels. Le magasin embarqué DuckDB atterrit via `land_duckdb_native` dans `store_connection.py` — un seul appel `executemany` pour tout le lot, jamais une boucle ligne par ligne.

## Redirection de résultat volumineux {: #large-result-redirect }

Les résultats dépassant un seuil de lignes sont redirigés vers un stockage compatible S3 (MinIO) au lieu d'être retournés en ligne.

### Modes de redirection {: #redirect-modes }

| Mode | Fonctionnement | Les données touchent-elles Provisa ? |
| ------ | ------------- | ---------------------- |
| **CTAS** (Parquet, ORC) | Le moteur de fédération écrit directement vers S3 via `CREATE TABLE AS SELECT` | Non |
| **Téléversement Provisa** (JSON, NDJSON, CSV, Arrow IPC) | Provisa sérialise et téléverse via boto3 | Oui |

Pour les formats natifs CTAS, Provisa ne touche jamais aux données : le moteur de fédération écrit les fichiers directement dans MinIO/S3. C'est le chemin à privilégier pour les gros exports analytiques.

### Sans magasin d'objets {: #with-no-object-store }

Un résultat va dans le magasin d'objets lorsqu'il y en a un de configuré (un endpoint, ou des identifiants pour AWS S3) et que le moteur y écrit lui-même le format demandé. Sinon, il est écrit dans un fichier du dossier de l'organisation, `<data directory>/orgs/<org>/results/`. Un magasin configuré dans lequel le moteur écrit mais qu'il ne peut pas atteindre fait échouer la requête nommément. Le résultat n'est pas écrit dans le dossier à la place.

| Moteur | Écrit le fichier lui-même | Formats |
| ------ | ---------------------- | ------- |
| DuckDB | `COPY (query) TO '<file>'` | parquet, csv, json, ndjson |
| PostgreSQL | `COPY (query) TO STDOUT`, diffusé dans le fichier sur l'hôte de Provisa | csv |
| Tout autre format ou moteur | Provisa l'écrit à partir des lignes | parquet, orc, csv, arrow, json, ndjson |

Le handle a les mêmes clés que celui du magasin d'objets, avec `"sink": "local"` et une `redirect_url` égale à `/data/redirect-file/<name>`, un chemin sur le serveur qui a répondu. Seul l'utilisateur qui a demandé le résultat peut le récupérer. Chaque récupération compare l'identité propre de la requête au propriétaire enregistré à côté du fichier, et répond 403 à toute autre personne et 401 à une requête sans identité.

Un fichier plus ancien que `redirect.ttl` reçoit une réponse 404 et est supprimé. Un minuteur le supprime à son expiration, comme sont supprimés les résultats du magasin d'objets. Chaque nouveau résultat balaie aussi le dossier de résultats de l'organisation, ce qui retire les fichiers dont un redémarrage a interrompu le minuteur.

### En-têtes de redirection {: #redirect-headers }

| En-tête | Effet |
| -------- | -------- |
| `X-Provisa-Redirect-Format: <mime>` | Redirige dans ce format (implique force sauf si seuil défini) |
| `X-Provisa-Redirect-Threshold: N` | Redirige seulement si le résultat dépasse N lignes |
| `X-Provisa-Redirect: true` | Force la redirection en utilisant le format par défaut |

Ces en-têtes implémentent la redirection contrôlée par le client.

**Réponse :**

```json
{
  "data": {"orders": null},
  "redirect": {
    "redirect_url": "https://minio:9000/provisa-results/results/abc.parquet?...",
    "row_count": 50000,
    "expires_in": 3600,
    "content_type": "application/vnd.apache.parquet"
  }
}
```

### Configuration serveur {: #server-configuration }

| Variable d'env | Défaut | Objectif |
| --------- | --------- | --------- |
| `PROVISA_REDIRECT_ENABLED` | `false` | Active la redirection par seuil côté serveur |
| `PROVISA_REDIRECT_THRESHOLD` | `1000` | Seuil de nombre de lignes par défaut |
| `PROVISA_REDIRECT_FORMAT` | `parquet` | Format de redirection par défaut |
| `PROVISA_REDIRECT_BUCKET` | `provisa-results` | Nom de bucket S3 |
| `PROVISA_REDIRECT_ENDPOINT` | | URL d'endpoint compatible S3 |
| `PROVISA_REDIRECT_TTL` | `3600` | TTL de l'URL présignée (secondes) |

## Arbre de décision de routage {: #routing-decision-tree }

```text
Multi-source query? → Federation engine
NoSQL source (MongoDB, Cassandra)? → Federation engine
Uses path columns on non-PG source? → Federation engine
Single RDBMS with driver? → Direct (sub-100ms target)
Single RDBMS without driver? → Federation engine
Steward hint "federated"? → Federation engine (override)
Steward hint "direct"? → Direct (if possible)
Redirect to Parquet/ORC? → Federation engine (CTAS, regardless of source count)
```

## Optimisation de requête de fédération {: #federation-query-optimization }

Provisa alimente automatiquement l'optimiseur fondé sur les coûts du moteur de fédération, afin que les plans de requêtes inter-sources reposent sur la vraie répartition des données et non sur des valeurs par défaut figées.

### Statistiques automatiques (`ANALYZE`) {: #automatic-statistics-analyze }

À l'enregistrement de source, Provisa exécute `ANALYZE catalog.schema.table` pour chaque table publiée. Cela collecte :

- Nombre de lignes
- Par colonne : fraction de null, nombre de valeurs distinctes, min/max, histogrammes (selon le connecteur)

L'optimiseur utilise ceci pour estimer la sélectivité des requêtes filtrées. Sans statistiques, il se replie sur des défauts fixes (ex. 10% de sélectivité pour les prédicats d'égalité) qui produisent de mauvais plans de jointure sur des données asymétriques ou à cardinalité élevée. Avec les statistiques, les estimations sont assez précises pour prendre les bonnes décisions de jointure broadcast vs. partitionnée pour la plupart des charges de travail.

**Couverture** : la prise en charge des statistiques dépend du connecteur. PostgreSQL, MySQL, Hive, Iceberg et Delta Lake prennent entièrement en charge `ANALYZE` ; MongoDB et Cassandra en partie ou pas du tout. Un catalogue dont le connecteur ne collecte pas de statistiques est ignoré, et un `ANALYZE` qui échoue est journalisé puis ignoré, si bien que l'enregistrement n'est jamais bloqué.

**Limites de sélectivité** : les statistiques fournissent des estimations par colonne. Pour les prédicats corrélés (`WHERE region = 'US' AND city = 'Seattle'`), l'optimiseur suppose l'indépendance des colonnes, ce qui peut sous-estimer le nombre de lignes. C'est une limitation connue des statistiques au niveau colonne dans tous les optimiseurs basés sur les coûts.

**Sources API** : les tables `api_cache_{table_name}` en PostgreSQL sont analysées automatiquement après chaque cycle de rafraîchissement de cache, de sorte que l'optimiseur ait des estimations de lignes à jour lors de la jointure de sources adossées à API avec des sources relationnelles.

### Admin : rafraîchir les statistiques {: #admin-refresh-statistics }

Relancez la collecte de statistiques à la demande via l'API admin :

```graphql
mutation {
  refreshSourceStatistics(sourceId: "sales-pg") {
    tablesAnalyzed
    failures { table message }
  }
}
```

Utile quand une source a reçu des données nouvelles significatives depuis son enregistrement.

## Vues matérialisées {: #materialized-views }

Les MV optimisent de manière transparente les requêtes coûteuses en pré-calculant et mettant en cache les résultats.

### Relations comme indices de MV {: #relationships-as-mv-hints }

Une déclaration de relation est un artefact de gouvernance et aussi la description structurelle d'une forme de jointure. Cette forme (les tables, les colonnes, le type de jointure) est exactement ce dont l'optimiseur de MV a besoin ; une relation peut donc piloter directement la matérialisation.

Pour les **relations inter-sources**, cela se fait automatiquement au démarrage : chaque relation avec `materialize: true` dont les branches tombent dans plus d'une source génère une MV `JoinPattern` (`auto-mv-<rel_id>`), sans configuration de MV séparée. Quand le compilateur voit cette jointure dans une requête, le réécrivain substitue le résultat prématérialisé sans que l'appelant s'en aperçoive. Les relations au sein d'une même source ne génèrent rien, car ces JOIN sont déjà rapides en exécution directe.

Une **relation adossée à une jonction** matérialise tout son parcours. La table associative est une troisième branche : le motif porte donc le saut source, le saut de jonction et le discriminant qui fixe les lignes à un type d'arête, les colonnes propres de la jonction se retrouvant dans la vue à côté de celles de la cible. Comme la jonction compte comme une branche, une arête dont la jonction se trouve dans une autre source que les deux tables qu'elle relie est inter-sources, même quand ces deux-là partagent une source. Le réécrivain reconnaît les deux sauts comme une chaîne, où le second doit partir de l'alias introduit par le premier. Une requête qui atteint les deux mêmes tables sans passer par la jonction lit donc les tables de base, et une vue construite pour une valeur de discriminant ne répond jamais à un parcours filtré sur une autre.

La conséquence pratique : les stewards qui approuvent une relation décident implicitement si la jointure est un bon candidat pour la matérialisation. L'acte de gouvernance et l'indice d'optimisation sont la même déclaration.

### Modes

| Mode | Config | Comportement |
| ------ | -------- | ---------- |
| **Join-pattern** | `join_pattern` dans la config MV | Réécrit les JOIN correspondants pour lire depuis la table MV |
| **SQL personnalisé** | `sql` dans la config MV | SELECT arbitraire, optionnellement exposé en SDL |
| **Relation auto-matérialisée** | relation inter-sources (automatique) | Auto-génère une MV join-pattern ; aucune config requise |
| **Relation matérialisée par le steward** | `materialize: true` sur relation même-source | Opt-in explicite pour les chemins de jointure même-source chauds |

### Auto-matérialisation {: #auto-materialization }

Les JOIN inter-sources sont les requêtes les plus coûteuses (toujours fédérées). Les relations inter-sources génèrent automatiquement des définitions de MV au démarrage :

```yaml
relationships:
  - id: orders-to-reviews
    source_table_id: orders        # sales-pg
    target_table_id: product_reviews  # reviews-mongo
    source_column: product_id
    target_column: product_id
    cardinality: one-to-many
    materialize: true              # auto-create MV
    refresh_interval: 600          # refresh every 10 minutes
```

Seules les relations inter-sources génèrent des MV (les JOIN même-source sont déjà rapides via l'exécution directe). La MV démarre en statut `STALE` et est rafraîchie par la boucle de rafraîchissement en arrière-plan avant d'être utilisée par l'optimiseur de requête.

### Cycle de vie du rafraîchissement {: #refresh-lifecycle }

```text
STALE → (refresh loop picks up) → REFRESHING → FRESH
  ↑                                                |
  └──── mutation hits source table ────────────────┘
```

La boucle de rafraîchissement s'exécute toutes les 30 secondes, vérifie `get_due_for_refresh()`, et exécute `CREATE TABLE AS SELECT` (première exécution) ou `DELETE + INSERT` (suivantes) contre la table cible de MV via le moteur de fédération.

## Carte des modules {: #module-map }

| Module | Objectif |
| -------- | --------- |
| `api/` | App FastAPI, routeurs, middleware, gestion du cycle de vie |
| `api/flight/` | Serveur Arrow Flight (gRPC, port 8815) |
| `api/admin/` | API GraphQL admin Strawberry — config, découverte, vues |
| `api/rest/` | Endpoints REST auto-générés depuis les tables enregistrées |
| `api/jsonapi/` | Endpoints JSON:API auto-générés avec pagination et gestion d'erreurs |
| `api/data/subscribe.py` | Abonnements SSE — LISTEN/NOTIFY, polling, Debezium CDC |
| `compiler/` | Parsers GraphQL/SQL, générateur SQL sémantique, RLS, masquage, échantillonnage, gouvernance en deux étapes (`stage2.py`) |
| `cypher/` | Traducteur Cypher → SQL, parser, carte de label, traducteur d'écriture pour les mutations Cypher |
| `pgwire/` | Serveur protocole de fil PostgreSQL ; `catalog.py` intercepte pg_catalog/information_schema pour la visibilité d'objet par rôle |
| `vector/` | Recherche vectorielle — registre de modèles, fournisseurs d'embedding (openai/ollama/huggingface), traduction `cosine_similarity()`, index HNSW sur la colonne d'embedding d'une réplique, génération d'embedding déclarative |
| `compiler/federation.py` | Support de sous-graphe Apollo Federation v2 |
| `transpiler/` | Transpilation de dialecte, logique de routage |
| `executor/` | Exécution fédérée/directe, sérialisation, formats de sortie |
| `executor/drivers/` | Pilotes de source directs (PostgreSQL, MySQL, DuckDB, Snowflake, Databricks, ClickHouse, …) |
| `executor/trino_flight.py` | Client Flight SQL ADBC pour le moteur de fédération |
| `executor/ctas_write.py` | Redirection basée sur CTAS (le moteur de fédération écrit vers S3) |
| `executor/redirect.py` | Logique de redirection S3, téléversement côté Provisa |
| `federation/engine.py` | `FederationEngine`, `DriverClass`, `_ENGINE_BUILDERS`, `ENGINE_REGISTRY`, `build_engine` |
| `federation/connector.py` | Abstractions de connecteur — Trino, ClickHouse ; `Mechanism`, `WarehouseNativeConnector` |
| `federation/connector_duckdb.py` | Définitions de connecteur DuckDB et FDW PostgreSQL |
| `federation/snowflake_connectors.py` | Connecteurs ATTACH stage externe + table externe Snowflake |
| `federation/databricks_connectors.py` | Connecteurs ATTACH table externe UC Databricks |
| `federation/bigquery_connectors.py` | Connecteurs ATTACH BigQuery externe / BigLake |
| `federation/databricks_uc.py` | Auto-provisionnement d'identifiant Unity Catalog + emplacement externe |
| `federation/databricks_backend.py` | Backend d'exécution Databricks SQL warehouse |
| `federation/snowflake_backend.py` | Backend d'exécution Snowflake |
| `federation/bigquery_backend.py` | Backend d'exécution BigQuery (transport Arrow Storage Read API) |
| `federation/mssql_warehouse_backend.py` | Backends d'exécution Fabric Warehouse + Synapse (T-SQL sur ODBC) |
| `federation/mssql_warehouse_connectors.py` | Connecteurs ATTACH OPENROWSET pour Fabric / Synapse |
| `federation/fabric_shortcuts.py` | Auto-provisionnement de raccourci OneLake (connexion → lakehouse → raccourci) |
| `federation/clickhouse_backend.py` | Backend d'exécution ClickHouse |
| `federation/duckdb_backend.py` | Backend d'exécution DuckDB en processus |
| `federation/pg_backend.py` | Backend d'exécution PostgreSQL |
| `federation/store_connection.py` | Face d'écriture du magasin de matérialisation DuckDB-natif |
| `registry/` | Registre de requêtes persistées, gouvernance |
| `security/` | Visibilité, droits, masquage de colonne |
| `cache/` | Cache de résultat de requête adossé à Redis (palier chaud) |
| `mv/` | Registre de vues matérialisées, rafraîchissement, réécrivain SQL |
| `events/` | Événements de changement de jeu de données et dispatch de déclencheur |
| `webhooks/` | Exécution de webhook sortant pour mutations et événements |
| `scheduler/` | Gestion de tâche en arrière-plan basée sur APScheduler — déclencheurs cron et intervalle qui déclenchent des webhooks et des mutations, et les interrogations des sorties Kafka en direct |
| `apq/` | Protocole de fil Apollo APQ — cache de hash de requête adossé à Redis ; séparé de la mise en cache de résultat |
| `compiler/cursor.py` | Pagination par curseur de style Relay — arguments `first`/`after`/`last`/`before` et génération de `pageInfo` sur toutes les requêtes de liste |
| `compiler/aggregate_gen.py` | Types de requête `{table}_aggregate` auto-générés avec sous-champs `count`, `sum`, `avg`, `min`, `max` et accès `nodes` filtré |
| `compiler/enum_detect.py` | Auto-détection de type enum — types enum natifs PostgreSQL (`pg_enum`) exposés comme types enum GraphQL plutôt que scalaires chaîne |
| `compiler/hints.py` | Indices de performance de fédération — directives de routage au niveau requête embarquées comme commentaires SQL (`/* @provisa route=federated */`) qui surchargent le routage automatique |
| `compiler/mutation_gen.py` | Compilateur de mutation ; préréglages de colonne — valeurs statiques côté serveur ou de variable de session appliquées à l'insertion/mise à jour, non exposées dans le type d'entrée de mutation |
| `auth/approval_hook.py` | Hook d'approbation ABAC — autorisation externe enfichable appelée avant l'exécution de requête ; transports webhook, gRPC, et unix_socket ; scope par table/source/global ; politique de repli configurable |
| `subscriptions/` | État et livraison d'abonnement SSE |
| `discovery/` | Découverte de relation par LLM (API Claude) |
| `grpc/` | Génération proto, serveur gRPC, réflexion |
| `api_source/` | Sources API REST/GraphQL/gRPC avec cache PG |
| `kafka/` | Sources de sujet Kafka, sink, Schema Registry |
| `auth/` | Fournisseurs d'auth enfichables, middleware, mappage de rôle |
| `core/` | Config, modèles, DB, dépôts, secrets ; le modèle de rôle prend en charge `parent_role_id` et `flatten_roles()` pour l'héritage de rôle récursif |
| `hasura_v2/` | Convertisseur métadonnées Hasura v2 → config Provisa |
| `ddn/` | Convertisseur supergraph Hasura DDN → config Provisa |
| `mongodb/` | Connecteur de source MongoDB |
| `elasticsearch/` | Connecteur de source Elasticsearch |
| `cassandra/` | Connecteur de source Cassandra |
| `prometheus/` | Connecteur de source de métriques Prometheus |
| `source_adapters/` | Couche d'adaptateur générique pour les connexions de source |

## API Admin {: #admin-api }

L'API GraphQL admin Strawberry est montée sur `/admin/graphql` (port HTTP 8001). Elle est séparée de l'endpoint GraphQL de données et nécessite le rôle superuser ou admin.

| Capacité | Description |
| ----------- | ------------- |
| Téléchargement/téléversement de config | Exporte ou remplace la config YAML complète de Provisa |
| Éditeur de relation | Crée, met à jour, supprime des définitions de relation |
| Découverte FK par IA | Déclenche une analyse de candidat FK propulsée par Claude |
| Introspection de schéma | Parcourt les tables, colonnes, et rôles publiés |
| Gestion de vue | Enregistre et gère les définitions de vue matérialisée |

## Configuration des modèles IA {: #ai-models-configuration }

`GET /admin/ai-models` et `PUT /admin/ai-models` configurent le pipeline LLM pour chaque org.

Les paramètres sont **propres à chaque organisation** : les choix de chaque organisation se superposent à la configuration du déploiement et s'appliquent dès la requête suivante, sans redémarrage.

**Affectations de modèle par opération.** Six opérations ont chacune un vendeur et une chaîne de modèle configurables :

| Opération | Ce qu'elle pilote |
| --------- | -------------- |
| `table_description` | Descriptions de table générées par LLM |
| `column_description` | Descriptions de colonne générées par LLM |
| `relationship_inference` | Découverte de candidat FK |
| `sql_generation` | Génération NL → SQL |
| `table_selection` | Choix des tables à inclure dans le prompt NL |
| `mcp_chat` | La boucle d'utilisation d'outils du panneau de l'assistant de chat |

Le champ vendeur accepte tout vendeur compatible `aisuite` (`anthropic`, `openai`, `groq`, `mistral`, `cohere`, et d'autres) ou un endpoint local (`ollama`, `lmstudio`). Une chaîne de modèle vide retire la surcharge de l'org et revient au défaut de déploiement.

**Limite de débit NL.** Un plafond optionnel de requêtes par période appliqué par rôle. Les requêtes excédentaires retournent `429` avec `Retry-After`.

**Registre de modèle vectoriel.** Une liste de modèles d'embedding (champs : `id`, `provider`, `dimensions`, `api_key_env` et `base_url` optionnels, drapeau `enabled`). Remplacement de liste complète : chaque entrée doit avoir `id`, `provider`, et `dimensions` sinon l'écriture est rejetée `400`.

**Clés d'API.** La clé d'API LLM de chaque fournisseur est stockée chiffrée via `provisa.core.org_secrets` (voir plus bas). La réponse de `GET` indique seulement si une clé est définie pour chaque fournisseur et ne renvoie jamais la valeur. Envoyer une chaîne vide pour un fournisseur efface sa clé, et les appels LLM vers ce fournisseur reviennent à l'identifiant de la variable d'environnement du déploiement.

## Secrets chiffrés par org {: #per-org-encrypted-secrets }

`provisa/core/org_secrets.py` stocke les identifiants qui ne doivent jamais apparaître en clair dans la base de données. Il contient les clés API des fournisseurs d'IA (`{vendor}_api_key`) et la clé Jev (`jev_api_key`).

Les valeurs sont chiffrées avec l'`encryption_service` du processus, issu de `provisa.encryption.runtime`, le même mécanisme que `api_sources.auth`.

Douze vendeurs compatibles `aisuite` sont pris en charge : `anthropic`, `openai`, `cohere`, `groq`, `mistral`, `xai`, `deepseek`, `together`, `fireworks`, `nebius`, `sambanova`, et `inception`. Google, AWS, et Azure sont exclus car ils nécessitent une configuration au-delà d'une simple clé API (ID de projet, rôles IAM, région). Les vendeurs à endpoint local (`ollama`, `lmstudio`) n'ont pas de clé et sont exclus pour la même raison.

Passer `value=None` à `write_org_secret` supprime la ligne. Les appelants qui lisent un secret le consomment immédiatement (ex. pour construire un client LLM) et ne doivent l'échoer dans aucune réponse d'API.

## Endpoints REST et JSON:API auto-générés {: #auto-generated-rest-jsonapi-endpoints }

Les tables enregistrées sont exposées comme endpoints REST et JSON:API aux côtés de l'interface GraphQL.

| Interface | Chemin de montage | Spec |
| ----------- | ----------- | ------ |
| REST | `/rest/<table-id>` | GET/POST simple avec paramètres de requête |
| JSON:API | `/jsonapi/<table-id>` | conforme [jsonapi.org](https://jsonapi.org) — pagination, relations, objets d'erreur |

Ces endpoints appliquent le même pipeline de sécurité (RLS, masquage, vérifications de rôle) que l'endpoint GraphQL.

## Abonnements {: #subscriptions }

Les abonnements SSE sont servis à `GET /data/subscribe/{table}`. Trois modes de livraison :

| Mode | Mécanisme | Quand utilisé |
| ------ | ----------- | ----------- |
| **LISTEN/NOTIFY** | `LISTEN` PostgreSQL sur un canal | Sources PG avec activité de mutation |
| **Polling** | Réexécute la requête à intervalle | Sources non-PG, ou quand CDC indisponible |
| **Debezium CDC** | Sujet Kafka depuis un connecteur Debezium | Flux de changement à haute fréquence |

Le client reçoit `text/event-stream` avec un événement JSON par ligne modifiée ou diff.

## Système d'événements et de webhooks {: #event-webhook-system }

Les mutations de base de données (INSERT/UPDATE/DELETE) peuvent déclencher des événements sortants via les modules `events/` et `webhooks/`.

```text
Mutation executed → EventDispatcher → match event trigger rules
                                          ↓
                               WebhookExecutor → HTTP POST to configured URL
```

Les déclencheurs d'événement sont définis en config et appariés sur la table, le type d'opération, et un filtre de ligne optionnel. Les payloads de webhook incluent le type d'opération, la ligne modifiée, et le contexte de rôle.

## Services en arrière-plan {: #background-services }

Quatre boucles en arrière-plan démarrent pendant le cycle de vie de l'app (`api/app.py`) :

| Service | Intervalle | Objectif |
| --------- | ---------- | --------- |
| Boucle de rafraîchissement MV | 30 s | Interroge `get_due_for_refresh()`, exécute CTAS ou DELETE+INSERT sur les MV obsolètes |
| Évaluation de la réplication à chaud | `replication.hot_interval` | Promeut une table dont le nombre d'instructions a dépassé son seuil et demande sa réplique ; rétrograde celle qui est tombée sous la moitié. S'exécute dans le seul processus qui détient le verrou du planificateur |
| Chargeur de table chaude | Configurable | Charge les petites tables de référence en cache mémoire pour un accès sub-milliseconde |
| Poller de source API | Intervalle par source | Récupère et remet en cache les sources REST/GraphQL/gRPC distantes |

### Tables à chaud et tables répliquées en cas de charge {: #hot-tables-and-replicated-when-busy-tables }

| Palier | Stockage | Critères de promotion | Latence d'accès |
| ------ | --------- | ------------------- | ---------------- |
| Chaud | Mémoire en processus | Nombre de lignes < seuil, ou est une cible de relation | <1 ms |
| Répliquée en cas de charge | Réplique dans le magasin du moteur | `replicate` vaut Default ou Hot-N et les instructions gouvernées qui lisent la table ont dépassé son seuil | La lecture propre du moteur dans son magasin |
| Froid | Source distante | Défaut | 50–500 ms |

Une table relève au plus de l'un des deux : une table gérée par le niveau à chaud n'est pas répliquée pour cause de charge.

## Import de métadonnées (Hasura v2 / DDN) {: #metadata-import-hasura-v2-ddn }

Les déploiements Hasura existants peuvent être convertis en config Provisa sans réécriture manuelle.

| Module | Entrée | Sortie |
| -------- | ------- | -------- |
| `hasura_v2/` | `metadata.yaml` Hasura v2 | `config.yaml` Provisa |
| `ddn/` | JSON supergraph Hasura DDN | `config.yaml` Provisa |

Les deux convertisseurs mappent les tables suivies, relations, permissions, et schémas distants. Le résultat est une config Provisa complète prête au déploiement.

## Apollo Federation

`compiler/federation.py` expose Provisa comme sous-graphe Apollo Federation v2. Le SDL de sous-graphe est auto-généré depuis le schéma publié avec des directives `@key` sur les colonnes de clé primaire et des annotations `@external`/`@provides` sur les relations inter-sous-graphes. Provisa répond aux requêtes `_entities` et `_service` requises par la passerelle de fédération.

## Pagination par curseur {: #cursor-based-pagination }

Toutes les requêtes de liste prennent en charge la pagination par curseur de style Relay via `compiler/cursor.py`. Les clients passent les arguments `first`/`after` (avant) ou `last`/`before` (arrière). Le compilateur encode la position de ligne comme curseur base64 opaque et injecte les clauses `WHERE`/`LIMIT` appropriées. Chaque requête de liste retourne un objet `pageInfo` :

| Champ | Type | Description |
| ------- | ------ | ------------- |
| `hasNextPage` | Boolean | Vrai si plus de résultats existent après cette page |
| `hasPreviousPage` | Boolean | Vrai si des résultats existent avant cette page |
| `startCursor` | String | Curseur du premier nœud dans cette page |
| `endCursor` | String | Curseur du dernier nœud dans cette page |

## Requêtes d'agrégation {: #aggregate-queries }

Chaque table enregistrée obtient un champ racine `{table}_aggregate` auto-généré (`compiler/aggregate_gen.py`). Le type d'agrégation expose `count`, `sum`, `avg`, `min`, `max` par colonne numérique, et `nodes` pour un accès aux lignes filtré avec sélection de champ complète (même RLS/masquage que la requête de base). Un agrégat sur une seule table, depuis n'importe quelle surface (SQL ou compilée), est servi par une vue matérialisée fraîche qui le pré-agrège, sans rien déclarer : une vue dont le SQL est un `GROUP BY` sur une seule table de `SUM`, `COUNT`, `MIN` et `MAX` nommés sert les agrégats qui groupent par un sous-ensemble de ses colonnes de regroupement et ne filtrent que sur elles. Chaque agrégat est ré-agrégé sur les valeurs stockées (`COUNT` comme somme des comptes stockés, `AVG` à partir d'une somme et d'un compte stockés de la même colonne) ; ce que la vue ne peut pas servir exactement est laissé à la table. La réécriture s'exécute après la gouvernance, donc un filtre de lignes sur l'une des colonnes de regroupement de la vue est reporté sur la vue, et un filtre de lignes sur toute autre colonne, ou un masque sur une colonne sommée, laisse la lecture à la table.

## Requêtes persistées automatiques (APQ) {: #automatic-persisted-queries-apq }

`apq/cache.py` implémente le protocole de fil Apollo APQ. Quand un client envoie seulement un hash de requête (`extensions.persistedQuery`), Provisa le recherche dans Redis. En cas d'échec, il retourne une erreur `PersistedQueryNotFound` ; le client réessaie avec le corps complet de la requête, que Provisa stocke. C'est séparé de la mise en cache de résultat (`cache/`).

## Rôles hérités {: #inherited-roles }

Un rôle nomme au plus un parent via `parent_role_id`. Lors de la construction à l'exécution, `security/inheritance.py` parcourt la chaîne de chaque rôle de l'enfant vers le haut et la replie dans les propres copies des données chargées pour cette construction, de sorte que chaque recherche ultérieure reste indexée par l'id du rôle agissant. Les capacités et l'accès aux domaines sont l'union de la chaîne. Le `visible_to` d'une colonne, chaque entrée de sa liste de masques, ainsi que le `visible_to` d'une métrique, d'une fonction ou d'un webhook, s'appliquent à un rôle lorsqu'ils le nomment lui ou un ancêtre. Les autorisations d'écriture d'une table se résolvent par opération de la même façon : un rôle sans autorisation pour une opération prend celle de l'ancêtre le plus proche, la sienne d'abord. La RLS se résout par table avec priorité à l'enfant : le rôle le plus proche dans la chaîne qui possède une règle pour la table — sa propre règle de table avant sa règle de domaine — fournit l'unique prédicat exécuté ; la règle d'un ancêtre n'est jamais combinée avec elle. Un rôle hérite de la limite de débit de son parent lorsqu'il n'en définit aucune. Un parent doit exister, ne peut pas être le rôle lui-même et ne peut pas fermer un cycle ; un rôle dont d'autres rôles héritent ne peut pas être supprimé tant qu'ils n'ont pas été rattachés à un autre parent.

## Hook d'approbation ABAC {: #abac-approval-hook }

`auth/approval_hook.py` est un hook d'autorisation enfichable invoqué avant l'exécution de requête, après RLS et masquage. Il s'intègre avec des moteurs de politique externes (OPA, services ABAC personnalisés).

| Paramètre | Description |
| --------- | ------------- |
| Transport | `webhook` (HTTP POST), `grpc`, ou `unix_socket` |
| Scope | Par table, par source, ou global |
| Politique de repli | `allow` ou `deny` quand l'endpoint du hook est injoignable |

## Auto-détection de type enum {: #enum-type-auto-detection }

`compiler/enum_detect.py` examine les types enum natifs de PostgreSQL (`pg_enum`) à la génération du schéma. Une colonne d'un type enum PostgreSQL défini par l'utilisateur devient un type enum GraphQL, et ses valeurs deviennent des membres d'énumération au lieu de scalaires chaîne.

## Déclencheurs planifiés {: #scheduled-triggers }

`scheduler/jobs.py` utilise APScheduler pour exécuter des jobs en arrière-plan définis comme déclencheurs cron ou intervalle. Chaque job peut faire un POST vers une URL de webhook ou exécuter une mutation contre l'endpoint de données ; la sortie Kafka en direct d'une table (`live.outputs`) est interrogée sur le même planificateur. Les déclencheurs sont configurés via l'API admin (mutations `scheduledTrigger`) ou la clé `scheduled_triggers` dans la config YAML.

## Indices de performance de fédération {: #federation-performance-hints }

`compiler/hints.py` parse les indices de steward embarqués dans les requêtes comme commentaires en utilisant la syntaxe de commentaire de Provisa. Le format d'indice varie selon le langage de requête :

```graphql
# @provisa route=federated
{ orders { id amount } }
```

```sql
/* @provisa route=federated */
SELECT id, amount FROM orders
```

```cypher
// @provisa route=federated
MATCH (o:Order) RETURN o.id, o.amount
```

| Indice | Effet |
| ------ | -------- |
| `route=federated` | Force la fédération à travers le moteur de fédération, en contournant le routage par pilote direct |
| `route=direct` | Force l'exécution par pilote direct |
| `cache=true` | Activer le cache de réponses pour cette requête (forme commentaire SQL : `-- @provisa cache=true`) |
| `cache_ttl=N` | L'activer et conserver le résultat en cache N secondes (`-- @provisa cache_ttl=N`) |

Le cache de réponses s'active par requête : GraphQL utilise la directive `@cached(ttl: Int)`, SQL et Cypher le commentaire `cache`/`cache_ttl`, gRPC les métadonnées d'appel `x-provisa-cache` / `x-provisa-cache-ttl`. L'indication s'applique sur chaque transport qui porte le langage (Cypher sur HTTP, Bolt et Arrow Flight ; GraphQL sur HTTP et Arrow Flight). Une requête sans indication ne lit ni n'écrit jamais le cache, et aucune forme ne lit des données plus récentes que ne le permettent les paramètres de source et de table de l'opérateur. Voir [Cache](configuration.md#per-request-opt-in).

## Autorisations d'écriture et presets dans les mutations {: #write-grants-and-presets-in-mutations }

Les `write_grants` d'une table indiquent, par rôle et par opération, quelles colonnes une écriture peut nommer, quelles lignes elle peut toucher et quelles valeurs l'autorisation définit elle-même. `compiler/write_admission.py` admet chaque écriture à l'étape de gouvernance, de sorte que GraphQL, SQL, Cypher et les autres surfaces obtiennent la même réponse : le droit `write`, l'autorisation du rôle pour l'opération, les colonnes, les presets ajoutés à l'instruction, le prédicat `rows` combiné par AND dans le `WHERE` et le `check` évalué sur les nouvelles lignes avant que quoi que ce soit n'atteigne une source. Les presets proviennent d'une variable de session, d'un littéral ou de l'instant de l'écriture. L'instruction ne peut pas fournir une colonne à preset, donc les presets ne figurent pas dans le type d'entrée de mutation GraphQL généré.

## Explorateur de schéma GraphQL Voyager {: #graphql-voyager-schema-explorer }

L'interface d'administration (`provisa-ui/src/pages/SchemaExplorer.tsx`) intègre GraphQL Voyager comme visualisation interactive du schéma. Il dessine le schéma limité au rôle sous forme de diagramme entité-relation navigable, avec les tables comme nœuds et les relations comme arêtes. Le schéma affiché est toujours filtré sur le rôle actuellement sélectionné.

## Ordre d'application de la sécurité {: #security-enforcement-order }

L'interrogation n'a aucune barrière de capacité ; la gouvernance vient entièrement de contrôles au niveau des données. Une requête SQL brute est refusée (HTTP 403) pour toute table hors du périmètre d'objets du rôle, avant que la gouvernance ne s'exécute.

1. **Visibilité d'objet** : le schéma par rôle cache les tables/colonnes non autorisées ; les tables hors scope en SQL brut sont rejetées
2. **Application de relation** : les traversées doivent exister dans le catalogue de relations approuvées, sauf si le rôle possède `ignore_relationships` — parmi les rôles système amorcés, seul `modeler` l'a. En mode haute sécurité, la capacité est ignorée et aucune traversée n'échappe au catalogue
3. **RLS** : injection de clause WHERE par table par rôle
4. **Masquage de colonne** : transformation de données par colonne par rôle
5. **Plafond de ligne (LIMIT)** : plafond de nombre de lignes pour les rôles sans `full_results` ; l'échantillonnage statistique aléatoire est une fonctionnalité de requête utilisateur séparée

Les quatre interfaces de requête (HTTP, Flight, gRPC, pgwire) appliquent le même pipeline de gouvernance Stage 2 ; aucun chemin client ne peut le contourner sans contourner le serveur.

## Limites de scalabilité {: #scalability-limits }

Provisa est une fine couche de compilation et de routage, qui ajoute quelques millisecondes à la latence des requêtes. Les chemins où Provisa sérialise les données du résultat sont en revanche limités par la mémoire du processus. Deux chemins n'ont aucune limite :

| Chemin | Borné en mémoire ? | Adapté à |
| ------ | -------------- | ------------- |
| JSON en ligne (HTTP) | Oui | Résultats petits-moyens |
| **Streaming Arrow Flight (gRPC :8815)** | **Non** | **Non borné — streaming via Zaychik ou API Arrow d'entrepôt** |
| Protobuf gRPC en ligne (:50051) | Oui | Résultats moyens, service-à-service |
| Redirection : téléversement Provisa (JSON, CSV, NDJSON, Arrow IPC) | Oui | Résultats moyens, téléchargement de fichier |
| **Redirection : CTAS (Parquet, ORC)** | **Non** | **Non borné — le moteur de fédération écrit vers S3** |

### Sondage de seuil {: #threshold-probing }

Pour la redirection basée sur seuil, Provisa injecte `LIMIT threshold + 1` dans la requête comme sonde. Si le résultat a moins de lignes, il est retourné en ligne (résultat complet, aucun travail gaspillé). Si le résultat atteint la limite, la sonde est écartée et la requête complète est ré-exécutée via CTAS ou téléversement Provisa. Cela évite `SELECT COUNT(*)` (que certaines sources n'optimisent pas) et fonctionne sur toute source.

Pour les charges de travail analytiques volumineuses, utilisez soit :

- **Arrow Flight** (port 8815) pour le streaming vers les outils de données — les lots circulent à travers Provisa sans matérialiser
- **Redirection Parquet/ORC** pour les exports basés fichier — le moteur de fédération écrit directement vers S3, Provisa retourne une URL présignée

## Infrastructure

| Service | Image | Port | Objectif |
| --------- | ------- | ------ | --------- |
| API Provisa | (processus hôte) | 8001 | Endpoint HTTP/REST |
| Flight Provisa | (processus hôte) | 8815 | Serveur gRPC Arrow Flight |
| gRPC Provisa | (processus hôte) | 50051 | Serveur gRPC Protobuf |
| Moteur de fédération | `trinodb/trino` (défaut) ou entrepôt externe | 8080 / variable | Moteur de fédération de requête — Trino pour la pile embarquée ; Snowflake/Databricks/BigQuery/Fabric/Synapse/DuckDB pour les cibles d'entrepôt |
| Zaychik | `provisa-zaychik` (construit depuis la source) | 8480 | Proxy Arrow Flight SQL pour Trino ; non requis pour les moteurs d'entrepôt |
| PostgreSQL | `postgres:16` | 5432 | Métadonnées de config + catalogue Iceberg |
| MongoDB | `mongo:7` | 27017 | Source de données NoSQL de démo |
| MinIO | `minio/minio` | 9000/9001 | Stockage d'objet compatible S3 |
| Redis | `redis:7-alpine` | 6379 | Cache de résultat de requête |
| PgBouncer | `edoburu/pgbouncer` | 6432 | Pooling de connexion pour PG |
| Kafka | `confluentinc/cp-kafka:7.6.0` | 9092 | Sources de données en streaming |
| Schema Registry | `confluentinc/cp-schema-registry:7.6.0` | 8081 | Gestion de schéma Avro/Protobuf |
