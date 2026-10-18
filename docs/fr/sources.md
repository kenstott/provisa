# Types de sources {: #source-types }

## Modèle d'exécution {: #execution-model }

Toute requête finit par s'exécuter à travers le moteur de fédération, qui assure la fédération sur l'ensemble des sources. Les sources se répartissent en trois catégories selon leur connectivité. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Catégorie | Pilote direct | Connecteur fédéré | Exemples |
| --- | --- | --- | --- |
| **Capable en direct** | Oui | Oui | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Fédération seule** | Non | Oui | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (adossé à S3) |
| **Lecture directe (réplica)** | Oui | Oui | Snowflake, Databricks, ClickHouse — le pilote lit les données et dépose un réplica ; les requêtes s'exécutent contre le réplica dans le moteur actif |
| **Direct (sans connecteur inter-sources)** | Oui | Non | Athena — les instructions mono-source s'exécutent entièrement dans Athena ; les jointures inter-sources déposent un réplica complet |
| **Matérialisation → fédération** | Non | Non | REST/OpenAPI, GraphQL distant, gRPC, Cypher Neo4j, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (récepteur push), GovData, SharePoint, Salesforce, ServiceNow, inventaire cloud, Splunk, Qdrant, Azure Cosmos DB, Logs, Google Workspace, Microsoft 365 |

Les sources **capables en direct** exécutent les requêtes mono-source via leur pilote natif (moins de 100 ms), en contournant le moteur de fédération (REQ-027, REQ-229). Elles conservent la prise en charge complète des connecteurs et participent à la fédération lorsqu'elles sont jointes à d'autres sources (REQ-028).

Les sources en **fédération seule** sont toujours interrogées à travers la couche de fédération. Aucun pilote direct n'existe (REQ-229).

Les sources en **lecture directe (réplica)** disposent d'un DirectDriver qui lit depuis l'entrepôt nativement (nativement Arrow lorsque c'est possible), dépose un réplica dans le magasin de matérialisation du moteur actif, puis les requêtes s'exécutent contre ce réplica. Voir [Entrepôts de données comme sources nommées](#warehouses-as-named-sources).

Les sources **directes (sans connecteur inter-sources)** disposent d'un DirectDriver qui exécute les instructions mono-source entièrement dans la source — chaque filtre, chaque colonne et chaque limite est poussé vers elle. Lorsqu'une instruction joint la source à une autre, le pilote lit les données et dépose un réplica ; la jointure s'exécute contre ce réplica dans le moteur actif. Aucun connecteur Trino n'existe pour ces sources.

Les sources à **matérialisation** n'ont pas de connecteur fédéré. Provisa récupère leurs données (au démarrage ou au moment de la requête) et les met en cache en Parquet sur S3 ou dans PostgreSQL, les rendant atteignables par le moteur de fédération pour des requêtes inter-sources (REQ-309).

---

## Toutes les sources {: #all-sources }

Provisa enregistre **73** types de sources, ceux que propose le formulaire Sources. Les tableaux ci-dessous les couvrent tous les 73 ; l'index correspond au décompte. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `SOURCE_TYPES`, où `graphql` et `grpc` sont `graphql_remote` et `grpc_remote`; Kaggle, Wikipedia, GitHub, GitLab et Stripe sont comptés comme des sources distinctes bien qu'ils s'enregistrent en interne via les connecteurs `files`, `graphql_remote` et `openapi`]

| # | Groupe | Types de sources |
| --- | --- | --- |
| 1–15 | [SGBDR](#rdbms) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport`, `saphana`, `hiveserver2` |
| 16–24 | [Entrepôts de données cloud](#cloud-data-warehouses) | `snowflake`, `bigquery`, `databricks`, `redshift`, `athena`, `fabric`, `synapse`, `trino`, [`motherduck`](#motherduck) |
| 25–29 | [Analytique / OLAP](#analytics-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 30–33 | [Lac de données / formats de tables ouverts](#data-lake-open-table-formats) | `iceberg`, `delta_lake`, `hive`, `hive_s3` |
| 34–38 | [NoSQL](#nosql) | `mongodb`, `cosmosdb`, `qdrant`, `cassandra`, `redis` |
| 39–40 | [Sources de code](arch/code-sources.md) | `node`, `python` |
| 41–43 | [Flux (streaming)](#streaming) | `kafka`, `websocket`, `rss` |
| 44 | [Récepteur push](#push-receiver) | `ingest` |
| 45–46 | [Graphe et sémantique](#graph-semantic) | `neo4j`, `sparql` |
| 47–51 | [Fondées sur des fichiers](#file-based) | `sqlite`, `csv`, `parquet`, `files`, `logs` |
| 52–53 | [Observabilité et autres](#observability-other) | `google_sheets`, `prometheus` |
| 54–61 | [SaaS d'entreprise](#enterprise-saas-connectors) | `sharepoint`, `salesforce`, `servicenow`, `jira`, `cloudops`, `splunk`, `google_workspace`, `microsoft_365` |
| 62–67 | [Sources API](#api-sources) | `openapi`, `graphql_remote`, `grpc_remote`, GitHub, GitLab, Stripe |
| 68 | [GovData](#govdata) | `govdata` |
| 69–70 | [Vérificateurs de qualité des données](#data-quality-checkers-req-1443) | `soda`, `great_expectations` |
| 71 | [Profileur de données](#data-profiler) | `data_profiler` |
| 72 | [Jeux de données Kaggle](#kaggle-datasets) | Kaggle (mis en scène via le formulaire Sources ; s'enregistre comme source `files` — voir [Jeux de données Kaggle](#kaggle-datasets)) |
| 73 | [Wikipedia](#wikipedia) | Wikipedia (s'enregistre comme source `files` qui parcourt les pages nommées — voir [Wikipedia](#wikipedia)) |

Référence de chaque type de source pris en charge par Provisa. « Pilote direct » signifie que les requêtes mono-source s'exécutent nativement contre la source (moins de 100 ms) (REQ-027). Le « nom du connecteur » est le connecteur fédéré employé lorsque la source participe à des JOIN multi-sources (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### SGBDR {: #rdbms }

| Type de source | Pilote direct | Nom du connecteur | Dialecte | Mutations |
| ------------ | -------------- | ----------------- | ----------------- | ----------- |
| `postgresql` | asyncpg | postgresql | postgres | Oui |
| `mysql` | aiomysql | mysql | mysql | Oui |
| `mariadb` | aiomysql | mariadb | mysql | Oui |
| `singlestore` | — | singlestore | singlestore | Fédérées |
| `sqlserver` | aioodbc | sqlserver | tsql | Oui |
| `oracle` | oracledb | oracle | oracle | Oui |
| `duckdb` | duckdb | memory | duckdb | Oui |
| `cockroachdb` | asyncpg (protocole pg) | postgresql | postgres | Oui |
| `yugabytedb` | asyncpg (protocole pg) | postgresql | postgres | Oui |
| `greenplum` | asyncpg (protocole pg) | postgresql | postgres | Oui |
| `tidb` | aiomysql (protocole mysql) | mysql | mysql | Oui |
| `firebird` | — | — (extension DuckDB) | — | Non |
| `airport` | — | — (extension DuckDB) | — | Non |
| `saphana` | SQLAlchemy (`hana+hdbcli`) | — | — | Non |
| `hiveserver2` | impyla (HiveServer2 sur Thrift) | — | hive | Non |

Les bases compatibles au niveau du protocole réutilisent le pilote JDBC, le pilote asynchrone natif et le dialecte du protocole de base — CockroachDB, YugabyteDB et Greenplum empruntent le protocole PostgreSQL ; TiDB emprunte le protocole MySQL. Ils n'exigent que des entrées de registre, aucun nouveau code de connecteur. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) et `airport` (serveur Arrow Flight) sont des types de sources enregistrés atteints sur place via des extensions communautaires DuckDB lorsque DuckDB est le moteur actif — aucun pilote direct, aucun connecteur fédéré. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

#### `saphana`

SAP HANA est lu par son propre pilote et répliqué dans le magasin du moteur actif ; il est donc atteignable sur tous les moteurs. Le formulaire demande l'hôte, le port (39015 par défaut), le nom d'utilisateur, le mot de passe et la base ; sur un système multi-tenant, la base désigne le tenant. Register Table liste les schémas que l'utilisateur peut lire, les schémas système de HANA étant exclus. [tool-verified: `provisa/executor/drivers/registry.py` `_SQLALCHEMY_FALLBACK`; `provisa/api/admin/introspect.py` `_SAPHANA_SYSTEM_SCHEMAS`] (REQ-1753)

#### `hiveserver2`

Un point de terminaison HiveServer2 lu directement sur Thrift, pour un déploiement Hive où ce point de terminaison est la seule chose atteignable. Il se distingue de `hive` et de `hive_s3`, qui lisent les fichiers et le métastore de l'entrepôt sans jamais passer par HiveServer2. Les lignes sont lues à travers le point de terminaison et répliquées dans le magasin du moteur actif. Le formulaire demande l'hôte, le port (10000 par défaut), le nom d'utilisateur, le mot de passe et la base ; `federation_hints.auth_mechanism` choisit l'authentification (`PLAIN` par défaut, par exemple `GSSAPI` pour Kerberos). [tool-verified: `provisa/executor/drivers/hive.py` `HiveDriver`; `provisa/core/models.py` `SourceType.hiveserver2`] (REQ-1731)

### Entrepôts de données cloud {: #cloud-data-warehouses }

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Type de source | Pilote direct | Nom du connecteur | Dialecte | Mutations | Remarques |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Fédérées | Lit via snowflake-connector-python ; dépose un réplica ; `account`/`warehouse`/`role` dans `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Fédérées | Aucun DirectDriver ; atteint via le moteur de fédération ou l'ATTACH du moteur BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Fédérées | Lit via databricks-sql-connector (Cloud Fetch, Arrow) ; dépose un réplica ; `http_path` requis dans `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Fédérées | — |
| `athena` | AthenaDriver | — | athena | Partiel (INSERT toujours ; UPDATE/DELETE/MERGE sur Iceberg uniquement) | Requête serverless sur S3 ; les instructions mono-source s'exécutent entièrement dans Athena ; les jointures inter-sources atterrissent un réplica ; `region` requise dans `federation_hints` (REQ-1981) |
| `fabric` | MssqlWarehouseDriver | — | tsql | Fédérées | Microsoft Fabric Warehouse ; T-SQL sur TDS, authentification Azure AD ; dépose un réplica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Fédérées | Azure Synapse SQL ; T-SQL sur TDS, authentification Azure AD ; dépose un réplica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Fédérées | Lecture d'un coordinateur Trino/Presto distant via le dialecte SQLAlchemy trino ; dépose un réplica sur n'importe quel moteur (REQ-994) |

#### `athena`

Amazon Athena est un service de requêtes sans serveur sur des données dans S3, facturé par octet analysé. Une instruction portant uniquement sur des tables Athena est exécutée directement dans Athena : les filtres, les listes de colonnes et `LIMIT` sont transmis intégralement, de sorte que seuls les octets dont Athena a besoin pour répondre à cette instruction sont analysés et facturés. Une instruction qui joint une table Athena avec une autre source atterrit d'abord un réplica complet depuis Athena — toute la table est lue et Athena la facture — puis la jointure s'exécute contre ce réplica. Les paramètres Replicate et protection de charge de la source régissent la fréquence des atterrissages ; la note dans la documentation est la seule règle. (REQ-1981)

**Découverte.** Register Table appelle l'API de métadonnées d'Athena — `ListDatabases`, `ListTableMetadata`, `GetTableMetadata`. Ces appels n'exécutent aucune requête, de sorte que lister des bases de données et des tables et lire les colonnes d'une table ne coûte rien par octet. Les colonnes d'une table ne sont lues que lorsque cette table est sélectionnée. Les clés de partition suivent les colonnes régulières. Les types sont mappés via l'entrée `athena` de `provisa/core/ir_types.py` : `array<…>`, `map<…>` et `struct<…>` deviennent `text` ; un type inconnu est refusé en le nommant. [tool-verified: `provisa/federation/athena.py` `databases`, `tables`, `columns` ; `provisa/core/ir_types.py` entrée `"athena"`]

**Champs de la source.**

| Champ | Où | Requis | Par défaut | Notes |
| --- | --- | --- | --- | --- |
| `database` | source | Oui | — | La base de données Glue interrogée par défaut |
| `username` | source | Non | — | ID de clé d'accès AWS ; peut être `${secret:…}` ou `${env:…}` |
| `password` | source | Non | — | Clé d'accès secrète AWS ; peut être `${secret:…}` ou `${env:…}` |
| `region` | `federation_hints` | Oui | — | Région AWS, p. ex. `us-east-1` |
| `workgroup` | `federation_hints` | Non | `primary` | Groupe de travail Athena |
| `s3_staging_dir` | `federation_hints` | Non | défaut du groupe | Où Athena écrit les résultats ; requis si le groupe de travail n'en définit pas |
| `catalog` | `federation_hints` | Non | `AwsDataCatalog` | Catalogue de données Athena |
| `role_arn` | `federation_hints` | Non | — | ARN du rôle IAM assumé pour chaque appel |

`username` et `password` tous deux vides signifie que la chaîne de credentials boto3 est utilisée : rôle d'instance, variables d'environnement (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`) ou un profil nommé. L'un sans l'autre est refusé. [tool-verified: `provisa/executor/drivers/athena.py` `AthenaDriver.connect` ; `provisa/federation/athena.py` `athena_client`]

**Écritures.** Chaque table enregistrée accepte `INSERT`. `UPDATE`, `DELETE` et `MERGE` ne sont disponibles que sur une table Iceberg ; le format est enregistré à l'inscription (`registered_tables.table_format`). Une écriture sur une table Hive demandant `UPDATE`, `DELETE` ou `MERGE` est refusée en nommant la table et l'opération. Athena ne renvoie pas les lignes écrites — une clause `RETURNING` est refusée et GraphQL n'offre pas de champs de mutation pour ces tables ; `INSERT`, `UPDATE`, `DELETE` et `COPY` sont les chemins d'écriture. `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING` et `TRUNCATE` sont refusés en nommant la forme. [tool-verified: `provisa/executor/write_capability.py` `table_write_ops` ; `provisa/executor/writable.py` `_TYPE_RETURNS_NO_ROWS`, `_TYPE_REFUSED_FORMS`]

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  username: ${env:ATHENA_ACCESS_KEY_ID}
  password: ${env:ATHENA_SECRET_ACCESS_KEY}
  federation_hints:
    region: us-east-1
    workgroup: primary
    s3_staging_dir: s3://my-bucket/athena-results/
```

Avec un rôle assumé par appel (chaîne de credentials pour la session de base) :

```yaml
- id: analytics-athena
  type: athena
  database: my_glue_database
  federation_hints:
    region: us-east-1
    role_arn: arn:aws:iam::123456789012:role/AthenaQueryRole
    s3_staging_dir: s3://my-bucket/athena-results/
```


### MotherDuck

`motherduck` (REQ-1998) est une base de données DuckDB hébergée par MotherDuck. Sur le moteur DuckDB, elle est attachée sur place ; sur le moteur Postgres, elle reste le connecteur du foreign data wrapper `pg_duckdb` (REQ-1868). [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBMotherDuckConnector`, `PgDuckdbMotherDuckConnector`]

**Connexion.** `database` nomme la base de données MotherDuck, ou un partage par son adresse de partage. `password` est le jeton MotherDuck, résolu comme tout secret de source. [tool-verified: `DuckDBMotherDuckConnector.details`]

```yaml
- id: reference
  type: motherduck
  database: analytics            # attached as md:analytics
  password: ${secret:<name>}     # the MotherDuck token, or a reference to it
```

Le moteur DuckDB émet `ATTACH 'md:<database>'` ; pour un partage, `database` est l'adresse du partage (`_share/...`), l'attachement est donc `md:_share/...`. [tool-verified: `details` builds `"md:" + database`; `tests/integration/test_motherduck_duckdb_e2e.py` attaches MotherDuck's public sample-data share this way]

- **Gestion du jeton.** Le moteur se connecte avec `SET GLOBAL motherduck_token = ?`, le jeton étant passé comme valeur liée. Il ne figure dans le texte d'aucune instruction, donc aucun log, aucune trace ni aucune erreur ne le contient. [tool-verified: `provisa/federation/duckdb_runtime.py` `_sign_in_motherduck`]
- **Pas de jeton.** Une source sans `password` est refusée nommément avant que quoi que ce soit ne soit attaché (`a MotherDuck source signs in with its token (the password field); none is set`). Sans cette vérification, l'extension lancerait une connexion interactive et attendrait environ une minute. Une source sans `database` est également refusée. [tool-verified: `details`]
- **Un compte par moteur.** Un moteur se connecte à un seul compte MotherDuck. Une seconde source MotherDuck avec un jeton différent est refusée nommément : "one DuckDB engine reads one MotherDuck account". [tool-verified: `_sign_in_motherduck` compares a SHA-256 of the token]
- **Les écritures** passent par le moteur, comme sur le moteur Postgres. [tool-verified: `DuckDBMotherDuckConnector.capability` `write=True`]

**L'exécution hybride (duale)** ne demande aucun réglage. Sur le moteur DuckDB, toutes les sources sont attachées sur une même connexion, donc une instruction qui joint une table d'une source fichier DuckDB à une table MotherDuck s'exécute comme une seule instruction DuckDB, et MotherDuck décide où s'exécute chaque partie. [tool-verified: `DuckDBMotherDuckConnector` docstring; REQ-1998 notes record a live join showing dual-execution operators]

> **Déplacement des données.** En exécution duale, MotherDuck peut déplacer des lignes locales de son côté pour les parties qu'il exécute. Une telle instruction peut donc envoyer des données du fichier local vers le compte MotherDuck de l'organisation. Ne joignez pas un fichier contenant des données qui doivent rester sur le nœud à une source MotherDuck, sauf s'il est acceptable de les envoyer à ce compte. [tool-verified: `DuckDBMotherDuckConnector` docstring: "which may move local rows to its side"]

Une source MotherDuck n'est servie que par les moteurs DuckDB et Postgres. Aucun autre moteur n'a de connecteur pour elle, et rien n'y dépose ses tables comme réplicas. [tool-verified: `provisa/federation/engine.py` registers `DuckDBMotherDuckConnector` and `PgDuckdbMotherDuckConnector` and no other MotherDuck connector]

### Analytique / OLAP {: #analytics-olap }

[tool-verified: `executor/drivers/clickhouse.py`]

| Type de source | Pilote direct | Nom du connecteur | Dialecte | Mutations | Remarques |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Fédérées | Lit via clickhouse-connect (HTTP) ; `secure: "true"` dans `federation_hints` pour le TLS (REQ-986) |
| `druid` | — | druid | druid | Non | — |
| `exasol` | — | exasol | exasol | Non | — |
| `elasticsearch` | HTTP (moteurs natifs) | elasticsearch (Trino) | — | Non | Sur Trino, le connecteur le lit, ses propriétés provenant du DSL de mapping du type [tool-verified: `trino_connectors.py:309`] ; sur tout autre moteur, Provisa lit l'index via HTTP (index et mapping pour Register Table, une lecture par scroll pour l'atterrissage) et atterrit les lignes [tool-verified: `provisa/elasticsearch/fetch.py`, `provisa/events/source_loader.py` `make_elasticsearch_loader`] (REQ-1672) |
| `pinot` | — | pinot | — | Non | Connecteur Trino `pinot` ; `pinot.controller-urls` = host:port du contrôleur Pinot [tool-verified: `trino_connectors.py:199`] |

### Lac de données / formats de tables ouverts {: #data-lake-open-table-formats }

Ces types de sources relèvent de la fédération seule — aucun pilote direct, aucun dialecte. [tool-verified: `LAKE_ONLY_SOURCES` in `provisa/core/source_registry.py`] (REQ-229)

| Type de source | Nom du connecteur | Voyage dans le temps | Remarques |
| ------------ | ----------------- | ------------- | ------- |
| `iceberg` | iceberg | Oui (argument `as_of`, REQ-372) | — |
| `delta_lake` | delta_lake | Oui (argument `as_of`, REQ-372) | — |
| `hive` | hive | Non | — |
| `hudi` | — (moteur `Hudi` de ClickHouse, sans copie — REQ-1178) | Non | Aucun connecteur fédéré ; atteint sur place lorsque ClickHouse est le moteur actif |
| `hive_s3` | hive | Non | Hive adossé à S3 |

### NoSQL

`mongodb`, `cassandra` et `redis` disposent de connecteurs Trino (`redis` construit ses propriétés à partir du DSL de mapping du type). [tool-verified: `provisa/federation/trino_connectors.py`; `provisa/core/models.py`] (REQ-017, REQ-1097)

| Type de source | Nom du connecteur | Mutations |
| ------------ | ----------------- | ----------- |
| `mongodb` | mongodb | Non |
| `cassandra` | cassandra (Trino) ; lecture CQL via cassandra-driver sur tout autre moteur | Non | Les keyspaces sont des schémas ; Register Table liste les tables d'un keyspace et type les colonnes à partir des métadonnées de schéma du cluster (clés de partition comme clés primaires) ; l'extra `cassandra` installe le pilote [tool-verified: `provisa/cassandra/fetch.py`] (REQ-1676) |
| `redis` | redis (Trino) ; lecture redis-py sans HTTP sur tout autre moteur | Non | Un préfixe de clé `<table>:*` est une table et un hachage est une ligne ; Register Table liste les préfixes présents et type les colonnes d'un préfixe à partir de ses hachages (une entrée `mapping.tables` surcharge le motif, la colonne de clé, le type de valeur et les colonnes) [tool-verified: `provisa/redis/fetch.py`] (REQ-1675) |

**Redis sur les moteurs hébergés.** Redis n'est pas disponible quand le moteur de l'organisation est l'un des moteurs provisionnés de la plateforme hébergée. Trino lit les tables d'un catalogue Redis à partir de fichiers de description dans un répertoire propre au catalogue, et un moteur hébergé ne peut pas en recevoir un pendant qu'il tourne. Enregistrer ou appliquer une source Redis y est refusé nommément (HTTP 422, `sources.redis_needs_engine_files`) ; un déploiement auto-hébergé lit Redis comme décrit ci-dessus. [tool-verified: `provisa/core/repositories/source.py` `refuse_on_engine_shard`] (REQ-2073)

### Qdrant

`qdrant` (REQ-1983, REQ-2000) est lu et écrit par le client propre de Qdrant, `qdrant-client`. Qdrant est **live uniquement** : une propriété du type de source, pas un réglage. Il répond avec son index vectoriel et sa Query API, qu'aucun réplica ne peut reproduire ; ses tables ne sont donc jamais répliquées. Il n'y a pas de choix Replicate pour une source Qdrant ni pour ses tables ; une valeur `replicate` ou `row_materialize` sur l'une d'elles est refusée. [tool-verified: `provisa/core/source_registry.py` `LIVE_ONLY_TYPES` ; `provisa/api/admin/_landing_ttl.py` `_live_only_refusal`]

**Connexion.** `host` est l'URL de l'instance avec son port (`https://<cluster>.cloud.qdrant.io:6333`, ou un `http://qdrant:6333` auto-hébergé) ; `password` est sa clé d'API, une référence de secret résolue à la lecture de la source, vide pour une instance sans clé. `federation_hints.prefer_grpc: true` lit par le port gRPC de Qdrant plutôt que par REST. La source a un schéma, `default`. [tool-verified: `provisa/qdrant/fetch.py` `QdrantConnection.from_source` ; `provisa/qdrant/introspect.py` `SCHEMA`]

**Tables de collection.** Une collection est une table. Register Table propose les colonnes de la collection : `id` (l'identifiant du point, sa clé, chemin `@id`), les champs du payload (un champ indexé typé d'après son index, tout autre d'après ce que suggèrent les points échantillonnés ; les champs imbriqués aplatis avec leur chemin) et une colonne `json` par vecteur (chemin `@vector`, ou `@vector.<name>` pour un vecteur nommé), proposée non sélectionnée. Une colonne de vecteur nomme le modèle d'embedding enregistré dont elle contient les embeddings, choisi parmi les modèles dont les dimensions sont la taille du vecteur (dans Register Table et dans l'onglet Type et clés de l'éditeur) ; choisir un modèle enregistre le vecteur. [tool-verified: `provisa/qdrant/introspect.py` `columns` ; `provisa-ui/src/pages/tables/QdrantVectorColumns.tsx`]

**Instructions sur une source Qdrant.** Une instruction qui ne lit qu'une collection s'exécute dans Qdrant lorsque l'API de Qdrant l'exprime exactement :

- les conditions sur le payload deviennent un filtre Qdrant (`=`, `<>`, `IN`, intervalles sur nombres et dates, `IS NULL`, `AND`, `OR`, `NOT`) ; la sémantique de NULL de SQL est conservée, de sorte que `lang <> 'en'` ignore les points sans `lang` ;
- une condition sur `id` seul (`=` ou `IN`) est un `retrieve` ; toute autre lecture est un `scroll` filtré ;
- `ORDER BY cosine_similarity(<vector>, <query>) DESC LIMIT k` est un `query_points` sur un vecteur dont la distance est Cosine, le filtre étant appliqué avant le top-k. Un texte de requête est vectorisé par le modèle du vecteur. La réponse porte un avertissement indiquant que le top-k est approximatif, choisi via l'index de Qdrant.

Tout le reste (un agrégat, un `GROUP BY`, une colonne calculée ou masquée, un tri autre que le plus similaire d'abord) est traité par le moteur, comme ci-dessous. [tool-verified: `provisa/qdrant/direct.py` `analyse`, `condition`, `QdrantDriver`]

**Instructions traitées par le moteur.** Lorsqu'une instruction joint une collection à une autre source, ou calcule sur ses colonnes, la part de la collection est lue en direct depuis Qdrant pour cette instruction et remise au moteur sous forme de lignes. Cette part comprend les colonnes de la collection, les conditions de l'instruction portant sur la collection seule (dont ses règles de ligne) et, lorsque la collection est lue seule dans son `SELECT`, le tri par similarité et le `LIMIT` de ce `SELECT`. Une part que Qdrant ne peut pas exprimer exactement est refusée, en nommant la table et la raison. Une part sans condition, sans clé et sans top-k lirait toute la collection et est également refusée. Une condition sur un paramètre (`$1`) est laissée au moteur. [tool-verified: `provisa/federation/live_only.py` `cut`, `read` ; `provisa/pgwire/_pipeline.py` `_read_live_parts`]

**Écritures.** `INSERT`, `UPDATE` et `DELETE` sur une collection s'exécutent dans Qdrant :

- `INSERT … VALUES` nomme la colonne `id` et écrit de nouveaux points. Un `id` que la collection contient déjà est refusé, par son nom. La vérification lit d'abord les identifiants ; elle n'est donc pas atomique avec l'écriture.
- `INSERT … ON CONFLICT (id) DO UPDATE SET c = EXCLUDED.c, …` est un upsert. Les colonnes que l'instruction ne définit pas conservent leurs valeurs.
- `UPDATE` et `DELETE` agissent sur les points que leur `WHERE` sélectionne, sous forme de filtre Qdrant ; une condition sans forme de filtre est refusée.
- Un texte écrit dans une colonne de vecteur est vectorisé par le modèle de la colonne. Un vecteur qui y est écrit doit avoir les dimensions du modèle.

[tool-verified: `provisa/qdrant/direct.py` `_insert`, `_upsert_clause`, `_write_points`, `_update`, `_delete`, `_vector_value`]

**Tables de recherche.** La recherche vectorielle est une table de recherche : son `query_template` contient une requête de la Query API.

```json
{"collection": "docs", "query": "@query_vector", "using": "text",
 "filter": {"must": [{"key": "lang", "match": {"value": "@lang"}}]},
 "limit": "@k", "score_threshold": 0.5, "model": "text-embedding-3-small"}
```

- `query` est toujours un argument ; `using`, `filter`, `limit`, `score_threshold` et `model` sont optionnels. Toute valeur de `using`, `filter`, `limit` ou `score_threshold` qui vaut exactement `"@name"` est aussi un argument.
- Chaque argument est un paramètre de la table (`query_arg`, REQ-1986), converti strictement vers son type déclaré et placé dans la requête comme valeur JSON, jamais dans son texte. L'argument de `query` est `array(double)` et obligatoire.
- Un appelant passe les arguments comme n'importe quel paramètre : `SELECT id, score, title FROM docs_search WHERE _nf_query_vector = '[0.1, 0.2, …]' AND _nf_lang = 'en'` en SQL, arguments de champ en GraphQL.
- Un argument optionnel non donné ne filtre rien : sa condition est omise du filtre. Sans `limit`, une recherche renvoie la valeur par défaut de Qdrant, 10 points.
- Le filtre est appliqué par Qdrant avant le choix de ses points les plus proches ; il est donc exact. Toute autre condition de l'instruction restreint ensuite les points renvoyés par la recherche ; elle n'en récupère pas davantage.
- `score` est la métrique propre de la collection pour le vecteur recherché : pour une collection Cosine, la similarité cosinus. Register Table liste les vecteurs de la collection avec leur taille et leur distance.
- `model` nomme un modèle d'embedding enregistré (`vector_models`, admin → AI Models). Un texte donné pour la requête est alors encodé par ce modèle, et un vecteur donné doit avoir les dimensions de ce modèle (REQ-429, REQ-430). Sans `model`, la requête doit être un vecteur.

Une recherche est lue par instruction sous ses arguments, ses lignes conservées sous ces arguments pendant le TTL de la table. Une table de recherche est en lecture seule. [tool-verified: `provisa/qdrant/table.py` `check_table`, `plan`, `read` ; `provisa/api/data/materialization.py` `_is_native_read`]

### Flux (streaming) {: #streaming }

| Type de source | Mécanisme | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | Connecteur Kafka fédéré ; schéma via Confluent Schema Registry (Avro, Protobuf, JSON Schema), définition manuelle ou inférence sur échantillon (REQ-147, REQ-150) | Non ; la sortie en direct d'une table publie vers Kafka (REQ-565) |
| `websocket` | Flux WebSocket externe — connexion, abonnement, réception d'événements ; résultats matérialisés (REQ-338) | Non |
| `rss` | Flux RSS 2.0 / Atom — interrogation, filigrane par pubDate/updated ; résultats matérialisés (REQ-342, REQ-343) | Non |

**Kafka sur les moteurs hébergés.** Sur les moteurs provisionnés de la plateforme hébergée, une source Kafka doit nommer un registre de schémas (`schema_registry_url`). Une source dont seules les déclarations du modèle donnent les colonnes des topics est refusée nommément à l'enregistrement ou à l'application (HTTP 422, `sources.kafka_needs_schema_registry`), car ses descriptions de tables exigeraient un répertoire qu'on ne peut pas donner au moteur en cours d'exécution. Les identifiants SASL parviennent à ces moteurs en mémoire (voir [Sécurité](security.md#the-federation-engine-trino)). [tool-verified: `provisa/core/repositories/kafka_source.py` `refuse_on_engine_shard`] (REQ-2073)

### Récepteur push {: #push-receiver }

| Type de source | Mécanisme | Mutations |
| ------------ | ----------- | ----------- |
| `ingest` | Des services externes POSTent des événements JSON ; résultats matérialisés (REQ-331, REQ-335) | Non |

### Graphe et sémantique {: #graph-semantic }

| Type de source | Mécanisme | Mutations |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via API HTTP, résultats mis en cache dans PostgreSQL (REQ-295) | Non |
| `sparql` | SPARQL 1.1 en POST, résultats mis en cache dans PostgreSQL (REQ-297) | Non |

### Fondées sur des fichiers {: #file-based }

Trois mécanismes couvrent les fichiers. Les sources à fichier unique et le robot d'exploration de fichiers emploient le champ `path` au lieu de `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553) Le troisième, [Logs](#logs), désigne ses fichiers par un glob dans son `mapping` et ne lit que des fichiers du serveur Provisa. [tool-verified: `provisa/logs/settings.py`]

**Sources base de données fichier et à fichier unique** — `sqlite`, `duckdb`, `csv`, `parquet` font pointer `path` vers un seul fichier. (Une source `duckdb` garde son emplacement dans le champ `database` du formulaire.) [tool-verified: `provisa-ui/src/pages/sources/FileLocationFields.tsx`]

| Type de source | Emplacements | Mutations |
| --- | --- | --- |
| `sqlite` | chemin local, `https://`, `s3://`, `gs://`, `az://` | Oui sur un fichier local ; aucune sur un fichier distant |
| `duckdb` | chemin local, `https://`, `s3://`, `gs://`, `az://` | Oui sur un fichier local ; aucune sur un fichier distant |
| `csv` | chemin local, `https://`, `s3://`, `gs://`, `az://` (moteur DuckDB) | Non |
| `parquet` | chemin local, `https://`, `s3://`, `gs://`, `az://` (moteur DuckDB) | Non |

[tool-verified: `provisa/federation/connector_duckdb.py` `location_kind`, `file_location_details`; `provisa/executor/write_capability.py` `table_write_ops`. The table describes the DuckDB engine, where the connectors above take every location. On other engines, CSV and Parquet over `s3://` or `http(s)://` are better registered through the `files` source. [inferred]] (REQ-1997)

Un chemin avec un autre schéma (`ftp://`, par exemple) est refusé : "DuckDB reads no file at a `ftp://` location". [tool-verified: `location_kind` raises `ValueError`]

**Identifiants pour un emplacement distant.** Placez-les dans les `federation_hints` de la source. Chacun est limité au bucket ou au conteneur du chemin lui-même, donc les identifiants d'une source ne servent jamais pour l'emplacement d'une autre source. Une source qui n'en donne aucun est lue comme un emplacement public. [tool-verified: `object_store_secret_ddl`]

| Emplacement | Clés `federation_hints` |
| --- | --- |
| `s3://` | `access_key_id`, `secret_access_key` ; facultatifs `region`, `endpoint` (un stockage compatible S3 comme R2 ou MinIO) |
| `gs://` | `gcs_access_id`, `gcs_secret_key` (une clé HMAC) |
| `az://` | `azure_account_name`, `azure_account_key` |

**Les fichiers SQLite et DuckDB distants sont en lecture seule.** Le moteur DuckDB les attache sur place avec `READ_ONLY`, et une table sur l'un d'eux n'offre ni insert, ni update, ni delete. Sur un moteur qui n'attache pas SQLite, un fichier SQLite distant est lu par une connexion DuckDB temporaire qui l'attache en lecture seule, et ses lignes sont déposées comme d'habitude. [tool-verified: `_attach_options`; `table_write_ops`; `provisa/events/source_loader.py` `make_sqlite_loader` `_remote`]

**Téléverser un fichier.** Dans le formulaire d'une source `sqlite`, `duckdb`, `csv` ou `parquet`, téléversez le fichier au lieu de saisir un chemin. Le fichier est écrit en flux sur le disque, jamais gardé en mémoire, dans le dossier de l'organisation sous `files/<source id>/<file name>`, et le chemin de la source pointe vers lui. [tool-verified: `provisa/core/source_uploads.py` `receive`, `upload_dir`; `provisa/api/admin/source_files_router.py` `upload_file`]

- **Remplacer.** Un nouveau téléversement prend la place du fichier précédent, quel que soit son nom, et la source est provisionnée à nouveau pour que la lecture suivante voie le nouveau fichier. [tool-verified: `receive` removes other files in the directory; `upload_file` calls `reprovision_source_after_registration`]
- **Supprimer.** Supprimer la source supprime son fichier téléversé. Rien d'autre ne le supprime. [tool-verified: `provisa/api/admin/schema_mutation.py` `source_uploads.remove(id)`]
- **Qui.** Seuls les rôles dotés de la capacité `source_registration` peuvent téléverser. [tool-verified: `upload_file` `require_capability_request(request, "source_registration")`]
- **Taille maximale.** Le réglage `sources.max_upload_bytes` (environnement `PROVISA_SOURCES_MAX_UPLOAD_BYTES`) plafonne le fichier, 1 Gio (1073741824 octets) par défaut, minimum 1, lu au début d'un téléversement. Un fichier plus grand est refusé avec HTTP 413 en nommant la limite, et rien n'en est conservé. [tool-verified: `provisa/core/settings_catalog.py`; `UploadTooLarge`]
- **Chemin.** Tant que le chemin est le fichier téléversé de la source, le champ de chemin du formulaire est en lecture seule. [tool-verified: `FileLocationFields.tsx` `readOnly={uploaded}`]
- **Noms.** L'id de la source et le nom du fichier ne peuvent contenir que des lettres, des chiffres, `.`, `_` et `-`. [tool-verified: `source_uploads._SEGMENT`]
- **Stockage.** [inferred from the `source_uploads` docstring] Un fichier téléversé est stocké comme tout fichier de l'organisation : dans le dossier de l'organisation, chiffré quand ce dossier l'est, et visible par chaque nœud sur un montage partagé.

**Écritures dans un fichier SQLite ou DuckDB local, et verrous.** Une écriture prend le verrou du fichier et ne l'attend jamais. Quand un autre processus ou nœud détient le fichier, l'écriture est refusée, en nommant la source et en indiquant que son fichier est verrouillé. [tool-verified: `provisa/federation/file_locks.py` `FileLocked`; REQ-1997 notes quote the maintainer: "writes back to file databases in a multi-node env - just error them out when they can't a lock on it."]

- **SQLite.** L'écriture définit `PRAGMA busy_timeout = 0`, donc un fichier verrouillé échoue immédiatement. [tool-verified: `provisa/executor/drivers/sqlalchemy_driver.py` `_write_sqlite`]
- **DuckDB.** DuckDB ne partage un fichier qu'entre des ouvertures en lecture seule. L'attachement du moteur et la connexion propre de la source tiennent donc le fichier en lecture seule, et chaque nœud le lit en même temps. Une écriture met en pause les détenteurs de ce processus, ouvre le fichier en lecture-écriture pour cette seule instruction, puis le ferme. Si l'ouverture ne peut pas prendre le verrou, l'écriture est refusée. [tool-verified: `provisa/executor/drivers/duckdb_driver.py` `_write`: `with file_locks.exclusive(self._database):` then `duckdb.connect(self._database)`; `connector_duckdb._attach_options`]

**Montages partagés.** Dans un dossier d'organisation chiffré dont le montage est partagé par plusieurs nœuds (réglage `org_files.shared_mount`), une écriture dans un fichier SQLite ou DuckDB de ce dossier est refusée d'emblée, en nommant la source. La couche de chiffrement des dossiers (gocryptfs) ne transmet pas les verrous de fichiers aux autres nœuds, donc rien n'empêcherait deux nœuds d'écrire en même temps. Les lectures fonctionnent toujours. [tool-verified: `file_locks.refuse_unprotected`, `UnprotectedWrite`; both drivers call it before a write]

Un dossier partagé sans chiffrement doit être monté avec des verrous qui atteignent chaque nœud : un montage NFS avec le verrouillage activé, ni `nolock` ni `local_lock`. Aucune sonde ne le vérifie ; c'est une exigence de déploiement. [tool-verified: REQ-1997 description in `docs/arch/requirements.yaml`; no check exists in source [inferred]]

**Robot d'exploration de fichiers (source `files`)** — fait pointer `path` vers un motif glob, le parcourt récursivement et enregistre le répertoire comme catalogue fédéré de tables. Elle lit de nombreux formats sur de nombreux transports ; les ensembles ci-dessous proviennent du connecteur de fichiers (fork kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formats | Transports |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, et documents convertis en tables — HTML, Markdown, DOCX, PPTX | Système de fichiers local, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST et Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

Toute source `files` peut aussi déclarer un `crawl` dans son mapping, avec les mêmes réglages que ceux listés sous [Wikipedia](#wikipedia) : la source lit alors d'abord des pages web et dépose chaque tableau de données HTML et chaque fichier de données lié dans son répertoire sous forme de table. [tool-verified: `provisa/file_source/crawl.py` `crawl_operand`]

Sur le moteur DuckDB, `files` est lu nativement — une vue de scan `read_csv_auto` par `<table>.csv` sous le répertoire résolu (REQ-229) [tool-verified: `provisa/federation/connector_duckdb.py` `DuckDBFilesConnector`]. Sur un moteur sans connecteur `files` propre, les lignes atterrissent via le même serveur pgwire Calcite intégré au connecteur (`pgwire-file`) qu'utilisent sharepoint/splunk (REQ-954) — voir [Connecteurs SaaS d'entreprise](#enterprise-saas-connectors) ci-dessous. La couverture de bout en bout de l'interface (formulaire Sources → Register Table → requête SQL) et le chemin d'atterrissage pgwire sont validés dans REQ-1694.

#### Logs (REQ-1996) {: #logs }

Une source `logs` transforme des fichiers de journaux du serveur Provisa en une table. Vous désignez les fichiers par un chemin complet ou un glob, et vous choisissez le format dans lequel les enregistrements sont écrits. Chaque enregistrement devient une ligne. Un enregistrement que le format ne sait pas lire est conservé, avec son texte brut et la raison, et n'est jamais écarté. [tool-verified: `provisa/logs/__init__.py`; `provisa/logs/files.py` `rows_of`]

```yaml
sources:
  - id: mac-install-log
    type: logs
    mapping:
      glob: /var/log/install.log*   # the live file and its rotated, compressed copies
      format: macos_install
```

Le mapping accepte quatre clés et refuse toute autre en la nommant : `glob` et `format` (toutes deux obligatoires), puis `pattern` et `record_start`, que seul le format `regex` accepte. [tool-verified: `provisa/logs/settings.py` `parse`; `provisa/logparse/__init__.py` `format_for`]

```yaml
    mapping:
      glob: /var/log/myapp/*.log
      format: regex
      pattern: '(?P<ts>\S+ \S+) (?P<level>[A-Z]+) (?P<message>.*)'
      record_start: '\d{4}-\d{2}-\d{2} '   # a stack trace joins the line it follows
```

Dans le formulaire Sources, une source Logs s'intitule « Log files » sous File. Le bouton **Preview** montre, avant tout enregistrement, les fichiers que le glob désigne, les colonnes que donne le format et les 20 premières lignes du fichier le plus récent. [tool-verified: `provisa-ui/src/pages/sources/constants.ts`; `provisa-ui/src/pages/sources/LogsFields.tsx`; `provisa/api/admin/logs_router.py` `PREVIEW_ROWS`]

**Le glob.** Il doit être un chemin complet ; `~` est développé, et un chemin relatif est refusé (« name the files by their full path »). Il est évalué avec `glob` de Python en mode non récursif : `**` se comporte donc comme `*` et correspond à l'intérieur d'un seul niveau de répertoire. Seuls les fichiers ordinaires sont retenus, dans l'ordre des noms. Un fichier vide est ignoré tant qu'il n'a pas d'octets. [tool-verified: `provisa/logs/roots.py` `checked_glob`; `provisa/logs/files.py` `matched`, `identify`] Le glob peut nommer une variable d'environnement, comme dans `${env:LOG_DIR}/*.log`. La source garde la référence telle qu'elle est écrite et la résout à chaque lecture des fichiers ; une variable non définie est refusée par son nom. [tool-verified: `provisa/logs/settings.py` `parse`; `tests/unit/test_logs_source.py` `test_a_glob_naming_a_variable_reads_where_it_points`]

**Les colonnes propres à chaque ligne.** Elles précèdent les colonnes du format :

| Colonne | Type | Contient |
| --- | --- | --- |
| `_file_id` | text | L'identité du fichier : une somme de contrôle de ses 4096 premiers octets, décompressés |
| `_file_name` | text | Le chemin sous lequel l'enregistrement a été lu |
| `_line_offset` | bigint | Décalage en octets de la première ligne de l'enregistrement dans le texte décompressé |
| `_entry` | bigint | Quelle entrée de l'enregistrement est la ligne : 0, sauf pour un enregistrement par lot comme le `Records` d'un fichier CloudTrail |
| `_raw` | text | L'enregistrement tel qu'il a été écrit |
| `_parse_error` | text | Pourquoi l'enregistrement, ou l'une de ses valeurs, n'a pas pu être lu ; NULL quand il l'a été |
| `_landed_at` | timestamp | Quand la ligne est arrivée |

`_file_id`, `_line_offset` et `_entry` forment la clé de la ligne : un enregistrement lu deux fois reste une seule ligne. [tool-verified: `provisa/logs/__init__.py` `META_COLUMNS`, `KEY`; `provisa/logs/preview.py` `columns`]

**Formats et préréglages.** La liste des formats les regroupe par famille ; le dernier choix, `regex`, est votre propre motif. [tool-verified: `provisa/logparse/__init__.py` `BUILT_IN`, `catalog`; `provisa/logparse/presets/__init__.py` `presets`; family labels from `provisa-ui/src/i18n/locales/en/logsFields.json`]

| Famille (libellé du formulaire) | Noms de formats |
| --- | --- |
| Common formats | `rfc3164`, `rfc5424`, `macos_install`, `clf`, `combined`, `jsonl`, `logfmt`, `w3c`, `cef`, `gelf` |
| Amazon Web Services | `aws_alb`, `aws_nlb`, `aws_elb`, `aws_cloudfront`, `aws_s3_access`, `aws_cloudtrail`, `aws_vpc_flow`, `aws_waf`, `aws_route53_resolver` |
| Databases and runtimes | `postgresql_stderr`, `postgresql_csvlog`, `postgresql_jsonlog`, `mysql_error`, `mysql_slow`, `mongodb`, `redis`, `log4j2_default`, `logback_default`, `python_logging` |
| Google | `gcp_cloud_logging`, `android_logcat`, `chrome_net_export` |
| Containers and Linux | `kubernetes_cri`, `journald_json`, `linux_auditd`, `linux_dmesg` |
| Microsoft | `ms_iis`, `ms_windows_firewall`, `ms_dns_debug`, `ms_dhcp_audit`, `ms_exchange_tracking`, `ms_sqlserver_errorlog`, `azure_storage_analytics`, `azure_monitor`, `windows_evtx` |
| Network security monitoring | `zeek_tsv`, `zeek_json`, `suricata_eve`, `snort_fast` |
| Web servers and proxies | `nginx_error`, `apache_error`, `haproxy_http`, `squid_access`, `envoy_access`, `caddy_access`, `traefik_access` |
| Your own | `regex` |

Cela fait 56 formats intégrés et `regex`. [tool-verified: `BUILT_IN` enumerated with the project interpreter]

**Comment un format obtient ses colonnes.** Les formats obtiennent leurs colonnes de quatre façons. [tool-verified: `provisa/logparse/formats.py`; `provisa/logparse/presets/_shared.py`; `provisa/logparse/coltypes.py`]

- **Colonnes fixes.** La plupart des formats énoncent leurs colonnes et leurs types. `rfc3164`, par exemple, donne `timestamp`, `facility`, `severity`, `host`, `process`, `pid` et `message` ; `facility` et `severity` sont calculés à partir de la valeur `<PRI>`. Une ligne syslog BSD ne porte pas d'année : l'année est donc celle de la dernière écriture du fichier, ou l'année précédente quand le mois de la ligne est postérieur à celui de cette écriture.
- **Champs nommés par l'enregistrement.** `jsonl` et `logfmt` tirent leurs colonnes des champs vus dans les premières lignes des fichiers (paramètre `logs.preview_lines`, 1000 par défaut). Chaque colonne reçoit le type le plus étroit où tiennent toutes les valeurs vues : boolean, puis integer, float, timestamp et json, sinon text. Un champ que porte un enregistrement ultérieur et qu'aucune colonne n'a va dans une colonne json `extra`.
- **Champs nommés par un en-tête.** `w3c` et les préréglages bâtis dessus lisent la ligne `#Fields:` de chaque fichier ; `date` et `time` sont réunis en une seule colonne `timestamp`. `aws_cloudfront`, `ms_iis`, `ms_windows_firewall`, `ms_exchange_tracking` et `zeek_tsv` lisent de la même façon les lignes d'en-tête `#`, et `aws_vpc_flow` lit une ligne d'en-tête qui commence par `version`. L'endroit où une ligne d'en-tête a été lue reste attaché au fichier : un fichier lu plus loin plus tard conserve donc ses champs.
- **`regex`.** Chaque groupe nommé, tel que `(?P<level>…)`, est une colonne de type text.

L'écran Register Table liste ces colonnes avec leurs types et marque la clé. [tool-verified: `provisa/api/admin/schema_query.py` `_logs_columns`] Quand les lignes sont lues, chaque valeur est convertie vers le type que sa colonne a dans la table enregistrée : un type modifié à cet endroit est donc celui qui s'applique. Une valeur que sa colonne ne peut pas contenir devient NULL, et sa raison va dans `_parse_error`. [tool-verified: `provisa/logs/loader.py` `_load`; `provisa/logs/files.py` `_typed`] [inferred: that the screen lets a steward change a suggested type, as REQ-1996 states]

Un enregistrement plus long que `logs.max_line_bytes` (65536 par défaut) garde ses premiers octets dans `_raw` et n'est pas analysé ; `_parse_error` donne sa longueur. Un fichier qui commence par une marque d'ordre des octets UTF-8 ou UTF-16 est lu dans cet encodage, et tout autre fichier en UTF-8. [tool-verified: `provisa/core/settings_catalog.py`; `provisa/logs/files.py` `rows_of`, `text_encoding`]

**Enregistrements sur plusieurs lignes.** Un format doté d'un motif de début d'enregistrement fait un enregistrement d'une ligne qui y correspond plus toutes les lignes suivantes qui n'y correspondent pas. Une stack trace reste avec son exception, et une entrée de requête lente reste entière. Les lignes avant la première correspondance forment un enregistrement à part ; le format le refuse en général, et la ligne est conservée. Les formats intégrés dotés d'un début d'enregistrement sont `rfc3164`, `macos_install`, `aws_cloudtrail`, `postgresql_stderr`, `postgresql_csvlog`, `mysql_error`, `mysql_slow`, `log4j2_default`, `logback_default`, `python_logging`, `chrome_net_export`, `ms_sqlserver_errorlog` et `azure_monitor`. Avec `regex`, définissez `record_start` vous-même. [tool-verified: `provisa/logparse/framing.py` `records`; `record_start` of each format enumerated with the project interpreter]

Deux formats lisent eux-mêmes leurs enregistrements au lieu de les lire ligne par ligne. `kubernetes_cri` réunit en un seul enregistrement les lignes partielles (`P`) que le runtime a découpées, et `windows_evtx` lit le fichier binaire `.evtx`. [tool-verified: `provisa/logparse/presets/linux.py` `KubernetesCri`; `frames_bytes` enumerated with the project interpreter]

Un fichier encore en cours d'écriture est lu jusqu'à sa dernière ligne complète. Pour un format multiligne, il est lu jusqu'au début de son dernier enregistrement, que la ligne suivante peut prolonger. Une fois que le fichier a passé 2 secondes sans écriture, ce dernier enregistrement est tenu pour complet. [tool-verified: `provisa/logs/files.py` `QUIET_SECONDS`, `read_file`]

**Rotation.** Un fichier est reconnu à une somme de contrôle de ses 4096 premiers octets, décompressés, et non à son nom. Quand `system.log` passe en rotation vers `system.log.0` puis est compressé en `system.log.0.gz`, chaque copie est le même fichier. La lecture reprend à l'octet qu'a atteint la dernière lecture : aucune ligne n'est donc lue deux fois. [tool-verified: `provisa/logs/files.py` module docstring, `identify`; `tests/unit/test_logs_source.py` `test_a_rotated_and_compressed_file_is_read_on_and_no_line_twice`]

- La compression est reconnue aux premiers octets du fichier, jamais à son nom : gzip et bzip2. Un fichier compressé est traité comme complet.
- Une copie en rotation et sa jumelle compressée, toutes deux désignées par le glob, sont lues une fois, depuis la compressée.
- Un fichier trouvé plus court que le décalage déjà lu a été tronqué : c'est un nouveau fichier, lu depuis son début. Les lignes de l'ancien restent.
- Les fichiers de moins de 4096 octets qui commencent de la même façon se distinguent par l'inode : ils restent donc deux fichiers.
- Un fichier que le glob ne désigne plus est oublié ; ses lignes restent comme historique.

[tool-verified: `provisa/logs/files.py` `compression`, `Found.complete`, `identify`; `provisa/logs/loader.py` module docstring; `tests/unit/test_logs_source.py`]

**Rafraîchissement et livraison en direct.** Une table Logs est une réplique. Rien ne surveille les fichiers. À la place, quand le signal de changement de la source est `probe` ou `ttl_probe`, sa sonde calcule un condensat du chemin, de l'inode, de la taille et de la dernière écriture de chaque fichier désigné ; quand le condensat diffère, un rafraîchissement est demandé. Les types de sonde qu'accepte une source Logs sont `hash` et `none`. [tool-verified: `provisa/logs/loader.py` `files_token`, `_freshness_probe`; `provisa/events/probes.py` `_LOADER_PROBED`, `probe_capabilities`]

Un rafraîchissement ne lit que ce qui est nouveau. Chaque fichier est lu à partir de là où s'est arrêté le dernier passage, et les lignes sont écrites par leur clé, 5000 à la fois, avant que la nouvelle position soit conservée. Un passage qui s'arrête entre les deux étapes relit ces enregistrements, ce qui laisse les mêmes lignes. Chaque passage estampille ses lignes dans `_landed_at`, la colonne que la livraison en direct utilise comme filigrane. Une définition de table modifiée entraîne une reconstruction complète, qui lit chaque fichier depuis son début. [tool-verified: `provisa/logs/loader.py` `_UnitsRead`, `_WholeRead`, `BATCH_ROWS`, `replica_stamp_column`; `provisa/federation/replica_builds.py` `_kept_in_place`] [inferred: that subscriptions deliver a Logs table's new rows by `_landed_at`, as REQ-1996 states; the subscription side was not traced]

**Racines autorisées.** Le paramètre d'opérateur `logs.allowed_roots` liste les répertoires dans lesquels un glob peut se résoudre. Sa valeur par défaut est `["/var/log"]` ; c'est une liste de la carte Security d'Admin → Paramètres, qui se règle aussi par `PROVISA_LOGS_ALLOWED_ROOTS`, et toute modification demande confirmation. Chaque racine est résolue après développement de `~` et suivi des liens symboliques. [tool-verified: `provisa/core/settings_catalog.py` `logs.allowed_roots`; `provisa/logs/roots.py` `allowed_roots`]

Deux contrôles s'appliquent. La partie fixe du glob, jusqu'à son premier caractère générique, doit se résoudre dans une racine ; cela est vérifié à l'enregistrement de la source, puis à chaque aperçu et à chaque lecture. Chaque fichier désigné doit aussi se résoudre dans une racine une fois ses liens symboliques suivis : un lien symbolique qui sort de `/var/log` est donc refusé. Le refus nomme le chemin, ce vers quoi il s'est résolu et les racines autorisées :

```text
/home/me/app.log resolves to /home/me/app.log, outside the directories log files may be read from: /var/log
```

À l'enregistrement, le code est `schema.logs_path_refused` ; à l'aperçu, c'est un 403 avec `logs.path_refused`. [tool-verified: `provisa/logs/roots.py` `checked_glob`, `checked_file`; `provisa/api/admin/schema_common.py` `logs_refusal`; `provisa/api/admin/logs_router.py`; `tests/unit/test_logs_source.py` `test_a_symlink_escaping_the_root_is_refused_by_name`]

**Refus en mode hébergé (SaaS).** Un déploiement qui héberge plusieurs organisations ne lit aucun fichier de journal qui lui soit propre. Chaque contrôle de chemin refuse en premier, à l'enregistrement, à l'aperçu et à la lecture, avec « `<path>`: a server that holds many organizations reads no log file of its own ». Les fichiers appartiennent au serveur, non à une organisation en particulier : aucun locataire ne peut donc les lire. Le déploiement sait s'il héberge plusieurs organisations d'après son état de multilocation. [tool-verified: `provisa/logs/roots.py` `_refuse_on_saas`; `provisa/core/secrets_providers.py` `many_organisations`; `provisa/api/app.py` `set_many_organisations_resolver(lambda: state.multitenancy)`; `tests/unit/test_logs_source.py` `test_a_server_of_many_organizations_reads_no_log_file_of_its_own`]

#### Jeux de données Kaggle (REQ-1780, REQ-1781, REQ-1782, REQ-1783) {: #kaggle-datasets }

Kaggle est une plateforme de téléchargement de fichiers. Un jeu de données Kaggle mis en scène s'enregistre comme une source de type `files` et est interrogé via le même connecteur pgwire-file que toute autre source `files` — il n'existe aucun `SourceType` `kaggle` dans l'énumération. [tool-verified: `provisa/kaggle/downloader.py`; `provisa/core/models.py` `SourceType` — aucun littéral `kaggle`]

**Ajouter un jeu de données.** Ouvrez Sources → Abonnements → Kaggle. Le formulaire exécute deux étapes séquentielles, car le point de terminaison de recherche de jeux de données de Kaggle exige une authentification — le sélecteur ne peut apparaître avant la validation du jeton. [tool-verified: `provisa-ui/src/pages/sources/KaggleFormSection.tsx`] (REQ-1783)

1. **Jeton** — saisissez un jeton API Kaggle et cliquez sur « Valider ». La validation appelle `POST https://www.kaggle.com/api/v1/datasets/create/new` avec un corps vide. Un `401` signifie invalide ; toute autre réponse signifie valide (la propre validation de charge utile de Kaggle se déclenche avant la création d'un quelconque jeu de données — rien n'est persisté). [tool-verified: `provisa/kaggle/client.py` `validate_token`] (REQ-1782)
2. **Sélecteur** — un champ de recherche en direct interroge `GET /api/v1/datasets/list`. Chaque résultat affiche le titre et la description du jeu de données. Sélectionnez-en un, puis cliquez sur « Add Dataset ».

Cliquer sur « Add Dataset » télécharge le paquet depuis `GET /api/v1/datasets/download/{owner}/{ref}`, décompresse les membres CSV et Parquet dans `<PROVISA_DATA_DIR>/kaggle/<owner>/<ref>/<file-stem>/<file-name>`, et crée **une seule** source de type `files` dont le `path` est ce répertoire racine. Le jeton n'est jamais stocké côté serveur. [tool-verified: `provisa/kaggle/downloader.py` `stage_dataset`; `provisa-ui/src/pages/sources/KaggleFormSection.tsx` `handleConfirmDataset`] (REQ-1780, REQ-1781)

Après l'ajout de la source, enregistrez ses tables via l'écran normal Register Table. La découverte récursive de répertoires du connecteur pgwire-file liste chaque fichier comme sa propre table — le même mécanisme qu'utilise toute autre source `files` (REQ-1690). (REQ-1783)

**Limitation v1.** Un paquet contenant un fichier `.sqlite` ou `.db` est rejeté d'emblée avec une erreur claire. Seuls les fichiers CSV et Parquet sont mis en scène. [tool-verified: `provisa/kaggle/downloader.py` `UnsupportedKaggleDataset`, `_UNSUPPORTED_EXTENSIONS`]

**Nommage des tables.** Une table parcourue se nomme `<page>__<name>` : le titre de la page, puis la légende de la table ou, à défaut, le dernier intitulé au-dessus de la table, tous deux dans l'orthographe des noms de colonnes de la source. Par exemple, la table sous l'intitulé « Tallest buildings in the world » de la page `List_of_tallest_buildings` est `list_of_tallest_buildings__tallest_buildings_in_the_world`. Un fichier de données lié garde son propre nom. [tool-verified: a live crawl of that page through the 0.109.0 file bundle]

**Rafraîchissement.** Pour retélécharger un jeu de données après que Kaggle publie une nouvelle version, appelez la mutation GraphQL `refreshKaggleSource` avec l'ID de la source et un jeton valide. Cela remet les fichiers en scène sur place et évince le cache de point de terminaison pgwire-file afin que le connecteur capte tout changement de schéma à sa prochaine requête. Les `kaggle_owner` et `kaggle_ref` stockés dans `federation_hints` à la création identifient quel jeu de données retélécharger. [tool-verified: `provisa/api/admin/schema_mutation.py` `refresh_kaggle_source`] (REQ-1780)

**Aucun chemin de configuration YAML statique.** Les sources Kaggle sont créées uniquement via le formulaire Sources. Une source Kaggle exportée en YAML apparaît comme `type: files` avec `kaggle_owner` et `kaggle_ref` dans `federation_hints`. Retélécharger depuis Kaggle nécessite le flux de rafraîchissement de l'interface ou la mutation `refreshKaggleSource` — pointer le `path` YAML vers un répertoire déjà mis en scène est l'alternative pour les environnements isolés (air-gapped).


#### Wikipedia (REQ-1960) {: #wikipedia }

Wikipedia parcourt les pages que vous nommez. Chaque tableau de données d'une page, et chaque fichier CSV, TSV, Excel, JSON ou Parquet lié, devient une table de la source. Comme Kaggle, elle s'enregistre comme source `files` avec un `crawl` dans son mapping. L'ajouter n'enregistre rien : un steward enregistre les tables voulues via l'écran normal Register Table. [tool-verified: `provisa/file_source/wikipedia.py` `crawl_settings`, `DATA_FILE_EXTENSIONS`; `provisa-ui/src/pages/sources/constants.ts` `wikipedia: "files"`] (REQ-316)

**Champs de configuration.**

| Champ | Par défaut | Signification |
| --- | --- | --- |
| Pages to start from | aucun (obligatoire) | Une par ligne : un titre de page, ou l'adresse d'une page de l'édition choisie |
| Language edition | `en` | Le code de l'édition, par exemple `en`, `de` ou `ja` |
| Link depth | `1` | À combien de liens d'une page de départ on lit ; `0` ne lit que les pages de départ |
| Page limit | `25` | Le plus grand nombre de pages qu'un parcours lit |
| Contact | vide | Envoyé avec chaque requête pour que Wikipedia puisse vous joindre, comme l'exige sa politique |
| Follow "See also" links | désactivé | Désactivé, cette section est exclue de l'article |
| What this edition calls its "See also" section | vide | Demandé seulement quand la case ci-dessus est désactivée et que Provisa ne peut pas nommer la section pour l'édition choisie. Le titre de la section tel qu'une page l'affiche, par exemple `Voir aussi` |
| Directory for the crawl's files | vide | Où les tables et les fichiers de données sont écrits. Vide : `<PROVISA_DATA_DIR>/crawl/<source id>` |

[tool-verified: `provisa-ui/src/pages/sources/WikipediaFields.tsx`; `provisa-ui/src/i18n/locales/en/sourceFormFieldsExtended.json` `wp*`; `provisa/file_source/wikipedia.py` `DEFAULTS`; `provisa/file_source/crawl.py` `crawl_landing_directory`]

Provisa connaît l'intitulé « See also » de ces éditions : `en`, `de`, `es`, `it`, `pt`, `nl`, `pl`, `sv`, `ja`, `zh`, `ru`. Pour toute autre édition avec « See also » désactivé, le formulaire demande comment l'édition nomme cette section ; saisissez son titre tel qu'une page l'affiche (par exemple `Voir aussi`). Une source enregistrée sans lui est refusée (`wikipedia.see_also_heading_needed`). [tool-verified: `provisa/file_source/wikipedia.py` `SEE_ALSO_HEADING`, `crawl_settings`]

**Quels liens sont suivis.** Un lien est retenu d'après ce que le balisage de la page dit de lui, jamais d'après un nom dans son adresse : de tels noms diffèrent dans chaque édition linguistique et figurent aussi dans des titres ordinaires. [tool-verified: `provisa/file_source/wikipedia.py` module docstring]

- Suivis : les liens vers d'autres articles situés dans le texte, les listes et les tableaux de données de l'article. La marque du balisage est `a[rel='mw:WikiLink']`.
- Non suivis : les pages pas encore écrites, la page elle-même, les recherches de sources d'ouvrages, les pages de fichiers, les autres wikis et les sites externes.
- Pas lus du tout (retirés avant que les liens et les tableaux soient collectés) : les boîtes de navigation, les infobox, les notes et références, les catégories, la table des matières, les images et les messages de maintenance.
- Pas lus sauf si vous activez l'option : la section « See also ».

Seuls les tableaux que les éditeurs marquent comme tableaux de données (`table.wikitable`) deviennent des tables, et un tableau de moins de 2 lignes n'est pas proposé. [tool-verified: `provisa/file_source/wikipedia.py` `LINK_SELECTOR`, `REMOVE_SELECTORS`, `TABLE_SELECTOR`, `DEFAULTS`]

**Réglages avancés du parcours.** Le formulaire les garde repliés sous « Crawl settings » et affiche chacun avec sa valeur par défaut ; laissez un champ vide pour la conserver. Dans un mapping `files`, les mêmes noms vont sous `crawl:`, et un nom hors de cette liste est refusé. [tool-verified: `provisa/file_source/crawl.py` `CRAWL_SETTINGS`, `crawl_operand`; `provisa/file_source/wikipedia.py` `crawl_settings`]

| Réglage | Valeur par défaut pour Wikipedia | Signification |
| --- | --- | --- |
| `start_urls` | les pages que vous avez nommées | Pages d'où part le parcours |
| `max_depth` | `1` | Profondeur de liens |
| `max_pages` | `25` | Limite de pages |
| `request_delay` | `1 seconds` | Délai entre les requêtes |
| `user_agent` | `Provisa/<version> (+https://provisa.dev) file-crawler`, avec votre contact ajouté | Le nom que le parcours se donne |
| `content_selector` | `#mw-content-text .mw-parser-output` | Sélecteur CSS : les liens et les tableaux ne sont lus qu'à l'intérieur |
| `remove_selectors` | la liste des parties décrites plus haut | Sélecteurs CSS des parties exclues. L'indiquer remplace toute la liste, y compris la règle « See also » |
| `link_selector` | `a[rel='mw:WikiLink']` avec exclusions | Sélecteur CSS auquel un lien doit correspondre pour être suivi |
| `link_exclude_patterns` | vide | Expressions régulières ; un lien dont l'adresse en contient une n'est pas suivi |
| `follow_external_links` | `false` | Suivre les liens vers d'autres sites |
| `allowed_domains` | non défini | Domaines que le parcours peut visiter |
| `table_selector` | `table.wikitable` | Sélecteur CSS auquel un tableau doit correspondre |
| `generate_tables_from_html` | non défini | Si les tableaux HTML deviennent des tables |
| `html_table_min_rows` | `2` | Nombre minimal de lignes d'un tableau |
| `html_table_max_rows` | non défini | Nombre maximal de lignes d'un tableau |
| `allowed_file_extensions` | `csv`, `tsv`, `xlsx`, `xls`, `json`, `parquet` | Types de fichiers liés qui deviennent des tables |
| `html_cache_ttl` | `1 days` | Durée de conservation d'un parcours |
| `max_html_size` | non défini | Plus grande page lue, par exemple `10MB` |
| `max_data_file_size` | non défini | Plus grand fichier de données lu, par exemple `100MB` |

**Nommage des tables.** Une table parcourue se nomme `<page>__<name>`. Le nom est le propre `id` du tableau s'il en a un, sinon sa légende, sinon l'intitulé le plus proche au-dessus. Par exemple, un tableau sous l'intitulé « Tallest buildings » de la page `List_of_tallest_buildings` devient `list_of_tallest_buildings__tallest_buildings` après normalisation du nom. [tool-verified: calcite `file` adapter `HtmlToJsonConverter.java` (`baseFileName + "__" + tableName`), `HtmlTableScanner.java` `getTableName`] [inferred: the exact normalized spelling in the example]

**Rafraîchissement.** Un parcours est conservé pendant la période de cache, `1 days` par défaut (`html_cache_ttl`). Les pages ne sont récupérées de nouveau qu'une fois ce délai écoulé. [tool-verified: `provisa/file_source/wikipedia.py` `DEFAULTS`]


### Observabilité et autres {: #observability-other }

`prometheus` dispose d'un connecteur Trino (propriétés construites à partir du DSL de mapping du type). `google_sheets` est un type de source enregistré sans connecteur Trino, qui se matérialise via le pipeline de cache d'API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Type de source | Nom du connecteur | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (matérialisé) | Non |
| `prometheus` | prometheus | Non | Une métrique est une table et un échantillon est une ligne (`timestamp`, `value`, une colonne par étiquette) ; sur tout moteur sans connecteur en direct, Provisa lit l'API HTTP — noms de métriques et étiquettes pour Register Table, `query_range` sur la plage de la table pour l'atterrissage [tool-verified: `provisa/prometheus/fetch.py`] (REQ-1689)

### Connecteurs SaaS d'entreprise {: #enterprise-saas-connectors }

Google Workspace et Microsoft 365 sont lus par des chargeurs propres ; voir [Google Workspace et Microsoft 365](#google-workspace-and-microsoft-365). SharePoint, Salesforce, ServiceNow, Jira, l'inventaire cloud et Splunk s'enregistrent via des connecteurs Apache Calcite (fork kenstott/calcite). Aucun n'a de pilote direct — Provisa lance le serveur pgwire Calcite fourni avec le connecteur (`pgwire-sharepoint`, `pgwire-salesforce`, `pgwire-servicenow`, `pgwire-jira`, `pgwire-cloudops`, `pgwire-splunk`) et l'atteint comme un endpoint PostgreSQL générique. Sur le moteur DuckDB, cet endpoint est rattaché en direct via l'extension postgres : Register Table liste les tables du connecteur depuis le catalogue rattaché, les requêtes lisent le connecteur sur place, et les filtres et projections se repoussent vers Calcite (REQ-1690) [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBPgwireConnector`]. Tout autre moteur dépose les lignes dans le magasin de matérialisation pour la fédération (REQ-954). Les paquets sont récupérés par OS/architecture depuis la version épinglée de `kenstott/calcite` (`pgwire-<connector>-<version>-<os>-<arch>.tar.gz` ; macOS arm64, Linux x86_64, Windows x86_64) [tool-verified: `provisa/runtime_deps/pgwire_bundles.py`] Un opérateur peut à la place nommer un dossier de paquet compilé pour cet hôte, par connecteur, sous Admin → Paramètres → Serveurs des connecteurs (`connectors.<connector>.bundle_dir`) ; voir [Admin](admin.md).. Les cinq connecteurs activent toujours la correspondance de noms insensible à la casse, conformément aux sémantiques insensibles à la casse de chaque produit (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100 ; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Les listes SharePoint sont énumérées comme des schémas et exposées comme des tables interrogeables (REQ-726, REQ-731). Deux méthodes d'authentification : `CLIENT_CREDENTIALS` (par défaut) et par certificat via un certificat PFX (REQ-727). Les valeurs secrètes de `mapping` sont résolues par le moteur de secrets avant d'atteindre le connecteur (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Champ de la source | Propriété du connecteur | Remarques |
| --- | --- | --- |
| `base_url` ou `host` | `site-url` | URL du site SharePoint |
| `username` | `client-id` | Identifiant client de l'application Azure |
| `password` | `client-secret` | Secret client de l'application Azure |
| `database` | `tenant-id` | UUID du locataire Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (par défaut) ou `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Chemin du PFX quand `auth_type: CERTIFICATE` — doit être ABSOLU |
| `mapping.certificate_password` | `certificate-password` | Mot de passe du PFX — la clé doit être présente, chaîne vide pour un PFX sans mot de passe |

L'authentification par certificat sur les moteurs autres que Trino porte deux règles supplémentaires, toutes deux appliquées lors de la construction de l'opérande `model.json` du serveur pgwire Calcite (REQ-1693). `certificate_path` doit être absolu : le serveur s'exécute avec son répertoire de paquet comme répertoire de travail, donc un chemin relatif se résout à l'intérieur du cache runtime-deps et le PFX n'est pas trouvé. `certificate_password` doit être présent dans `mapping` même quand le PFX n'a pas de mot de passe, auquel cas c'est la chaîne vide — l'adaptateur Calcite rejette d'emblée un mot de passe nul, et une clé absente est traitée comme une erreur de configuration plutôt que silencieusement lue comme un mot de passe vide. Une valeur manquante ou relative lève `MissingConnectorConfig` en nommant le champ. [tool-verified: `provisa/federation/pgwire_replica.py` `_sharepoint_operand`]

Lorsque le connecteur n'expose pas `information_schema.columns`, enregistrez la table avec des définitions de colonnes explicites (obtenues depuis l'API Microsoft Graph) via la mutation `registerTable` (REQ-732).

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  password: ${env:SP_CLIENT_SECRET}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CLIENT_CREDENTIALS
```

Authentification par certificat, avec le chemin absolu et le mot de passe toujours présent :

```yaml
- id: hr-sharepoint
  type: sharepoint
  base_url: https://kenstott.sharepoint.com
  username: ${env:SP_CLIENT_ID}
  database: ${env:SP_TENANT_ID}
  mapping:
    auth_type: CERTIFICATE
    certificate_path: /etc/provisa/certs/sharepoint.pfx
    certificate_password: ${env:SP_CERT_PASSWORD}
```

#### `salesforce`

Chaque sObject que les identifiants peuvent interroger est une table que le steward peut enregistrer (`Account`, `Opportunity`, objets personnalisés `*__c`) ; rien n'est enregistré automatiquement (REQ-1946). Les colonnes et les types proviennent du describe du sObject. Les filtres, projections, tris et limites sont délégués à Salesforce sous forme de SOQL. [tool-verified: `provisa/federation/pgwire_replica.py` `_salesforce_operand`; `provisa/federation/trino_connectors.py` `TrinoSalesforceConnector`]

Une source Salesforce indique l'URL de connexion Mon domaine de l'organisation et un jeu d'identifiants complet, choisi par `mapping.auth_type`. L'URL de connexion est l'URL `https://…` complète (par exemple `https://acme.my.salesforce.com`) ; un hôte nu ou tout autre schéma est refusé, en nommant le champ. Un jeu incomplet est refusé à l'utilisation de la source, en nommant la valeur manquante. Chaque secret peut être une référence `${secret:…}` ou `${env:…}`. Un secret saisi en clair est stocké dans le coffre-fort de l'organisation, et la source conserve sa référence.

| Champ de la source | Propriété du connecteur | Remarques |
| --- | --- | --- |
| `base_url` ou `host` | `login-url` | L'URL Mon domaine de l'organisation (Configuration, puis Mon domaine). `login.salesforce.com` ne fonctionne pas avec le flux d'identifiants client |
| `username` | `client-id` | Clé client de l'application connectée |
| `password` | `client-secret` | Secret client de l'application connectée |
| `mapping.auth_type` | — | `CLIENT_CREDENTIALS` (par défaut), `USERNAME_PASSWORD` ou `ACCESS_TOKEN` |
| `mapping.sf_username`, `mapping.sf_password` | `username`, `password` | `USERNAME_PASSWORD` uniquement, avec la clé et le secret client |
| `mapping.security_token` | `security-token` | `USERNAME_PASSWORD` uniquement ; facultatif |
| `mapping.access_token`, `mapping.instance_url` | `access-token`, `instance-url` | `ACCESS_TOKEN` uniquement ; ni clé ni secret client |
| `mapping.api_version` | `api-version` | Facultatif, par exemple `v61.0` |

Pour `CLIENT_CREDENTIALS`, l'application connectée doit avoir *Enable Client Credentials Flow* et un utilisateur *Run As* ; chaque lecture et chaque écriture s'exécute avec les autorisations de cet utilisateur. Les organisations créées depuis Summer '23 bloquent par défaut le flux nom d'utilisateur et mot de passe.

**Lectures.** Sur Trino, la source est lue via le catalogue `salesforce`, qui ne fait que lire. Sur tout autre moteur, le moteur attache le serveur pgwire de la source et la lit sur place.

**Écritures.** `INSERT`, `UPDATE` et `DELETE` sur un sObject enregistré sont des mutations : gouvernées, auditées et traitées par environnement comme toute autre mutation. Sur tous les moteurs, Trino compris, une écriture s'exécute sur le serveur pgwire de la source ; sur Trino, ce serveur ne tourne que pour les écritures. Chaque instruction est envoyée à Salesforce et validée à son exécution : elle ne peut donc pas être annulée. Le serveur ne renvoie pas les lignes qu'il écrit : une instruction avec une clause `RETURNING` est refusée, et GraphQL n'offre aucun champ de mutation pour ces tables ; `INSERT`, `UPDATE`, `DELETE` et `COPY` en SQL sont les chemins d'écriture. Une écriture est une seule instruction simple sur une seule table : `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` et `TRUNCATE` sont refusés, en nommant la forme. Une source SharePoint s'écrit de la même façon. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`; `provisa/api/data/pgwire_write.py`]

**Démarrage.** Avant d'accepter des connexions, le serveur de la source lit les colonnes de chaque sObject, environ quatre minutes pour une organisation qui en compte 1 200. Ce coût est payé une fois : les résultats de describe sont conservés dans le répertoire d'état propre à la source, sous le répertoire de données de l'instance. Le serveur est lancé quand la source est enregistrée ou chargée. Sur Trino, le catalogue `salesforce` garde ses propres résultats de describe, un répertoire par source sous `/data/trino/cache/salesforce-describe` dans le coordinateur ; le coût n'y est donc payé qu'une fois lui aussi, lorsque ce chemin est un volume qui survit au conteneur : le volume `trino_cache` de la pile compose, ou le volume de cache du coordinateur du chart Helm (`trino.coordinator.cache.persistence`). Tant qu'il n'accepte pas de connexions, Register Table affiche le connecteur comme en cours de démarrage, et une écriture reçoit le même message au lieu d'attendre. [tool-verified: `provisa/federation/pgwire_replica.py` `build_model_json`, `start_endpoint`]

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    api_version: v61.0
```

Flux nom d'utilisateur et mot de passe :

```yaml
- id: sales-salesforce
  type: salesforce
  base_url: https://acme.my.salesforce.com
  username: ${env:SF_CONSUMER_KEY}
  password: ${env:SF_CONSUMER_SECRET}
  mapping:
    auth_type: USERNAME_PASSWORD
    sf_username: ops@acme.com
    sf_password: ${env:SF_PASSWORD}
    security_token: ${env:SF_SECURITY_TOKEN}
```

#### `servicenow`

Chaque table de l'instance est disponible pour l'enregistrement ; rien n'est enregistré automatiquement (REQ-1954). Une instance complète peut en compter des milliers ; Register Table les regroupe donc par thème — chaque thème est le nom d'affichage d'un paquet ServiceNow, ou « (sans paquet) » quand une table n'appartient à aucun. Chaque table est présentée comme `name — label`. Sélectionner une table lit ses colonnes depuis l'instance ; lister les tables ne le fait pas. [tool-verified: `provisa/federation/pgwire_replica.py` `servicenow_table_list`, `servicenow_columns`; `provisa/federation/servicenow.py` `TABLE_LIST`, `COLUMN_LIST`]

Une source ServiceNow indique l'URL complète `https://…` de l'instance ainsi qu'un nom d'utilisateur et un mot de passe. L'URL est prise depuis `base_url` ou `host` ; un hôte nu ou tout autre schéma est refusé, en nommant le champ (`instanceUrl`). Un nom d'utilisateur ou un mot de passe absent est refusé, en nommant le champ. Chaque valeur peut être une référence `${secret:…}` ou `${env:…}`. Un secret saisi en clair est stocké dans le coffre-fort de l'organisation, et la source conserve sa référence.

| Champ source | Propriété de l'adaptateur | Notes |
| --- | --- | --- |
| `base_url` ou `host` | `instanceUrl` | URL complète `https://…` de l'instance (par exemple `https://acme.service-now.com`). Un hôte nu ou un autre schéma est refusé |
| `username` | `username` | Nom d'utilisateur pour l'authentification de base |
| `password` | `password` | Mot de passe pour l'authentification de base |

[tool-verified: `provisa/federation/servicenow.py` `servicenow_instance_url`, `servicenow_settings`]

**Lectures.** Sur Trino, la source est lue via le catalogue `servicenow`, qui ne fait que lire. Sur tout autre moteur, le moteur rattache le serveur pgwire de la source et la lit sur place. Seules les colonnes sélectionnées sont demandées à l'instance (`sysparm_fields`). Un filtre est envoyé à l'instance (`sysparm_query`) uniquement sous une forme que l'adaptateur a vérifiée contre une instance réelle ; tout autre filtre, ainsi que chaque tri et limite, est appliqué aux lignes lues. [inferred: the adapter's requirements ledger (kenstott/calcite `docs/requirements/servicenow.yaml`, SNOW-022, SNOW-023, SNOW-030)]

Un champ de référence apparaît sous forme de deux colonnes : `<field>` contient le sys_id de l'enregistrement référencé et `<field>__display` contient la valeur d'affichage. Un champ vide est NULL. Un champ dont le type ne peut pas être mappé par l'adaptateur provoque une erreur nommant la table, le champ et le type. [inferred from adapter behavior ; non vérifié directement dans le code source Provisa lors de cette session]

La recherche de tables (langage naturel) correspond au nom, au libellé et au thème de chaque table. Une fois une table enregistrée, ses colonnes rejoignent la recherche. [tool-verified: `provisa/discovery/catalog_cache.py` docstring de `COLUMNS_PER_TABLE_KINDS`]

**Cycle de vie du serveur.** Le serveur ne sert que les tables enregistrées depuis lui. Enregistrer ou supprimer une table redémarre le serveur et réapprovisionne la source sur le moteur ; une instruction arrivant pendant ce redémarrage échoue. [tool-verified: `provisa/federation/pgwire_replica.py` `SERVES_REGISTERED_TABLES` ; `provisa/api/app_rebuild.py` `serve_registered_tables`]

**Écritures.** `INSERT`, `UPDATE` et `DELETE` sur une table enregistrée sont des mutations : gouvernées, auditées et traitées par environnement comme toute autre mutation. Sur tous les moteurs, Trino compris, une écriture s'exécute sur le serveur pgwire de la source. Le serveur renvoie la ligne que l'instance détient après chaque écriture ; une clause `RETURNING` est donc acceptée et GraphQL propose des champs de mutation pour ces tables. Une écriture est une seule instruction sur une table : `INSERT … ON CONFLICT`, `UPDATE … FROM`, `DELETE … USING`, `MERGE` et `TRUNCATE` sont refusés, en nommant la forme. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`, `PGWIRE_SERVER_RETURNS_ROWS`]

**Import sets.** Quand une table possède un transform map actif vers elle, Register Table propose ses import sets, et le steward peut lier la table à l'un d'eux (une table avec plusieurs maps nomme chacun). Les insertions sur une table liée passent par l'Import Set API de l'instance et la transformation de ce map ; les insertions sur une table non liée, ainsi que chaque mise à jour et suppression, sont écrites directement dans la table. Une ligne que la transformation refuse est rejetée, en la nommant ; les lignes déjà écrites restent écrites. La liaison est vérifiée à l'enregistrement de la table : elle doit être l'import set d'un transform map actif vers la table, sur une source ServiceNow. [tool-verified: `provisa/api/admin/_import_set.py` `table_import_set_refusal` ; `provisa/federation/servicenow.py` `servicenow_settings` ; `provisa/federation/pgwire_replica.py` `servicenow_transform_maps`]

```yaml
- id: helpdesk-sn
  type: servicenow
  base_url: https://acme.service-now.com
  username: ${env:SN_USERNAME}
  password: ${env:SN_PASSWORD}
```

#### `jira`

Un site Jira Cloud est lu et écrit par son propre adaptateur Calcite (REQ-2069). Une table d'une source Jira est un **formulaire** : un projet et un type de ticket, et les champs que lisent ses colonnes. Register Table liste chaque formulaire dont le compte peut créer un ticket, regroupés par projet ; sélectionner un formulaire lit ses champs depuis Jira, lister les formulaires ne le fait pas. Chaque formulaire a les colonnes `key` (sa clé primaire), `id`, `summary`, `status`, `created` et `updated`, puis une colonne par champ choisi, nommée comme l'adaptateur la nomme : le nom du champ en snake case (`Story Points` devient `story_points`). Un champ dont le nom est déjà celui d'une colonne est nommé par son identifiant de champ (`customfield_10099`), si bien que tout champ peut être choisi. Un champ utilisateur contient l'identifiant du compte ; son nom d'affichage est un champ à part (`assignee.displayName`, colonne `assignee_name`). Les colonnes que Jira écrit lui-même sont en lecture seule : `key`, `id`, `created`, `updated`, le nom d'affichage d'un champ utilisateur, et un champ d'une forme que l'adaptateur ne connaît pas, qu'il sert sous forme de texte JSON. L'enregistrer la marque « Read only: Jira sets it » dans sa description lorsqu'elle n'en a pas, et une écriture qui la définit est refusée par le serveur, en la nommant. [tool-verified: `provisa/federation/jira.py` `FIXED_COLUMNS`, `form_columns`; `provisa/federation/pgwire_replica.py` `jira_form_list`, `jira_fields`; `provisa/api/admin/introspect.py` `complete_jira_form`]

La table d'un formulaire nomme son projet et son type de ticket dans ses `arguments` (`project`, `issue_type`), et chaque colonne lit le champ que nomme son `path` (un identifiant de champ ou le nom d'un champ), ou le champ de son propre nom. Une table enregistrée depuis l'admin obtient les deux de Jira ; une configuration les indique. Une table Jira qui fixe autre chose, ou à laquelle l'un des deux manque, est refusée, en la nommant. [tool-verified: `provisa/federation/jira.py` `form_of`; `provisa/core/config_loader.py` `_validate_table_arguments`]

| Champ source | Propriété de l'adaptateur | Notes |
| --- | --- | --- |
| `base_url` ou `host` | `baseUrl` | Adresse complète `https://…` du site (par exemple `https://acme.atlassian.net`). Un hôte nu ou un autre schéma est refusé |
| `username` | `email` | E-mail du compte Atlassian |
| `password` | `apiToken` | Un jeton d'API de ce compte, créé sur id.atlassian.com sous Security, API tokens |

[tool-verified: `provisa/federation/jira.py` `jira_site_url`, `jira_settings`]

**Liens.** La table `issue_links` de la source contient les liens entre les tickets de ses formulaires : `link_id`, `link_type`, `outward_key` et `inward_key`, chaque lien une seule fois. Une relation entre deux formulaires y est déclarée comme une junction (REQ-1586), son `link_type` servant de discriminant, si bien que GraphQL et Cypher passent d'un ticket au ticket lié selon le type de lien. [inferred: the adapter's design agreed for REQ-2069]

**Lectures.** Une recherche par clé (`key =`, `key IN`) est récupérée par clé, si bien qu'une clé qui ne nomme aucun ticket ne donne aucune ligne. Une comparaison de date ou de nombre est envoyée à Jira en JQL. Tout autre filtre, et tout tri, est appliqué aux lignes lues : une égalité sur un texte, une option, un statut ou un utilisateur n'est pas envoyée, car JQL refuse une valeur qui ne désigne rien là où SQL ne renvoie aucune ligne. Un champ à valeurs multiples (étiquettes, sélection multiple, composants, versions) est un texte contenant un tableau JSON, et s'écrit de la même façon. [inferred: the adapter's behaviour, as the calcite connector reports it for REQ-2069]

Sur DuckDB, PostgreSQL et ClickHouse, le moteur rattache le serveur pgwire de la source et la lit sur place. Sur Trino, la source n'a pas de catalogue propre et est chargée via son serveur. [tool-verified: `provisa/federation/engine.py`; `provisa/federation/strategy.py` `_CONNECTOR_PGWIRE_REPLICA`]

**Cycle de vie du serveur.** Le serveur ne sert que les formulaires enregistrés depuis lui, ainsi que sa liste de formulaires, sa liste de champs et les liens. Enregistrer, modifier ou supprimer un formulaire redémarre le serveur et réapprovisionne la source sur le moteur. [tool-verified: `provisa/federation/served_tables.py` `SERVES_REGISTERED_TABLES`, `serve_tables`; `provisa/api/app_rebuild.py` `serve_registered_tables`]

**Écritures.** `INSERT`, `UPDATE` et `DELETE` sur un formulaire sont des mutations, écrites sur le serveur pgwire de la source sur tous les moteurs. Les lignes sont insérées par lots de 50 ; une mise à jour ou une suppression se fait ticket par ticket, par clé. Définir `status` effectue la transition de workflow vers ce statut, et est refusé, en nommant les statuts atteignables, lorsque le workflow du ticket n'en a aucun. Un ticket ayant des sous-tâches n'est pas supprimé. Le serveur renvoie chaque ligne écrite par sa clé ; `RETURNING` est donc accepté et GraphQL propose des champs de mutation. Sur `issue_links`, `INSERT` crée un lien et `DELETE` en supprime un ; `UPDATE` est refusé. [tool-verified: `provisa/executor/writable.py` `PGWIRE_SERVER_WRITTEN`, `PGWIRE_SERVER_RETURNS_ROWS`] [inferred: the batching, transitions and subtask refusal are the adapter's, agreed for REQ-2069]

```yaml
- id: support-jira
  type: jira
  base_url: https://acme.atlassian.net
  username: ${env:JIRA_EMAIL}
  password: ${env:JIRA_API_TOKEN}
tables:
- source_id: support-jira
  table: incident
  arguments: {project: KAN, issue_type: Incident}
  columns:
  - {name: key, data_type: varchar, is_primary_key: true}
  - {name: summary, data_type: varchar}
  - {name: status, data_type: varchar}
  - {name: priority, data_type: varchar}
  - {name: flagged, data_type: varchar, path: customfield_10021}
```

#### `cloudops`

L'inventaire cloud présente les ressources d'Azure, d'AWS et de GCP sous la forme d'un seul jeu de tables : `compute_resources`, `storage_resources`, `kubernetes_clusters`, `database_resources`, `network_resources`, `iam_resources`, `container_registries` et `compute_security_groups`, qui relie une ressource de calcul à ses groupes de sécurité. Chaque table a un seul jeu de colonnes pour les trois clouds, et sa colonne `cloud_provider` nomme le cloud d'où provient une ligne. La source est en lecture seule, et ses tables sont enregistrées par un steward comme toute autre (REQ-1947). [tool-verified: `provisa/federation/cloudops.py` `cloudops_settings`; `provisa/federation/trino_connectors.py` `TrinoCloudopsConnector`]

Une source désigne un ou plusieurs clouds. Chaque cloud est tout ou rien : un cloud dont seules certaines valeurs obligatoires sont renseignées est refusé, en nommant ce qui manque, de même qu'une source qui ne désigne aucun cloud. La source lit exactement les clouds qu'elle désigne. Toutes les valeurs se trouvent dans `mapping`, et chaque secret peut être une référence `${secret:…}` ou `${env:…}`. Un secret saisi en clair est stocké dans le coffre-fort de l'organisation, et la source conserve sa référence.

| Cloud | Clé de `mapping` | Propriété du connecteur | Remarques |
| --- | --- | --- | --- |
| Azure | `azure_tenant_id` | `azure.tenant-id` | Obligatoire |
| Azure | `azure_client_id` | `azure.client-id` | Obligatoire ; l'identifiant client de l'enregistrement d'application |
| Azure | `azure_client_secret` | `azure.client-secret` | Obligatoire ; secret |
| Azure | `azure_subscription_ids` | `azure.subscription-ids` | Obligatoire ; séparés par des virgules |
| AWS | `aws_access_key_id` | `aws.access-key-id` | Obligatoire |
| AWS | `aws_secret_access_key` | `aws.secret-access-key` | Obligatoire ; secret |
| AWS | `aws_region` | `aws.region` | Obligatoire, par exemple `us-east-1` |
| AWS | `aws_account_ids` | `aws.account-ids` | Obligatoire ; séparés par des virgules |
| AWS | `aws_role_arn` | `aws.role-arn` | Facultatif ; un rôle à assumer pour l'accès inter-comptes |
| GCP | `gcp_credentials_path` | `gcp.credentials-path` | Obligatoire ; chemin ABSOLU vers un fichier de clé JSON de compte de service |
| GCP | `gcp_project_ids` | `gcp.project-ids` | Obligatoire ; séparés par des virgules |
| — | `cache_ttl_minutes` | `cache.ttl-minutes` | Facultatif ; durée de réutilisation d'un résultat avant d'interroger de nouveau les clouds (valeur par défaut de l'adaptateur : 5) |

Le fichier de clé GCP est ouvert par ce qui lit la source. Sur Trino, c'est le serveur Trino : le chemin doit donc y exister. Sur tout autre moteur, c'est le serveur pgwire de la source sur l'hôte Provisa, dont le répertoire de travail est son propre répertoire d'état ; un chemin relatif est donc refusé.

Sur Trino, la source est lue via le catalogue `cloudops`. Sur tout autre moteur, le moteur attache le serveur pgwire de la source et la lit sur place, comme une source SharePoint.

```yaml
- id: cloud-estate
  type: cloudops
  mapping:
    aws_access_key_id: ${env:CLOUDOPS_AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${env:CLOUDOPS_AWS_SECRET_ACCESS_KEY}
    aws_region: us-east-1
    aws_account_ids: "111111111111,222222222222"
    azure_tenant_id: ${env:CLOUDOPS_AZURE_TENANT_ID}
    azure_client_id: ${env:CLOUDOPS_AZURE_CLIENT_ID}
    azure_client_secret: ${env:CLOUDOPS_AZURE_CLIENT_SECRET}
    azure_subscription_ids: "sub-1,sub-2"
```

#### `splunk`

Les résultats de recherche Splunk sont interrogeables comme des tables (par exemple `internal_server`) (REQ-721). L'URL du connecteur provient de `base_url`, ou est construite comme `https://{host}:{port}` avec un port par défaut de `8089` (REQ-722). Authentification : quand `mapping.use_token` vaut `true` (le défaut), `password` est transmis comme jeton d'API ; quand il vaut `false`, `username` et `password` sont transmis comme identifiants distincts (REQ-723). [tool-verified: `provisa/federation/trino_connectors.py` lines 262–286]

| Champ de la source | Propriété du connecteur | Remarques |
| --- | --- | --- |
| `base_url` / `host` + `port` | `url` | `base_url`, sinon `https://host:port` (port 8089 par défaut) |
| `password` | `token` ou `password` | jeton quand `use_token: true` |
| `username` | `user` | uniquement quand `use_token: false` |
| `database` | `app` | restreindre à une application Splunk |
| `mapping.datamodel_filter` | `datamodel-filter` | filtrer sur un modèle de données |
| `mapping.disable_ssl_validation` | `disable-ssl-validation` | pour les certificats autosignés (REQ-724) |

Sur le chemin pgwire-replica (tout moteur sauf Trino), les quatre mêmes réglages optionnels deviennent les clés d'opérande `model.json` de Calcite `app`, `token`/`username`+`password`, `datamodelFilter` et `disableSslValidation` — les deux derniers dans les types vers lesquels `SplunkSchemaFactory` les convertit, une chaîne et un booléen (REQ-1694). [tool-verified: `provisa/federation/pgwire_replica.py` `_splunk_operand`]

```yaml
- id: ops-splunk
  type: splunk
  host: splunk
  port: 8089
  password: ${env:SPLUNK_TOKEN}
  mapping:
    use_token: true
    disable_ssl_validation: true
```

#### Google Workspace et Microsoft 365 (REQ-1923, REQ-1994) {: #google-workspace-and-microsoft-365 }

`google_workspace` et `microsoft_365` lisent le courrier, les agendas et les tâches des comptes qu'une source désigne, et les déposent comme répliques. Ce ne sont pas des connecteurs Calcite : chacun a son propre chargeur, qui appelle l'API du fournisseur, Gmail, Google Calendar et Google Tasks d'un côté, Microsoft Graph de l'autre. Le formulaire les liste sous Enterprise comme « Google Workspace (Gmail) » et « Microsoft 365 (Outlook, Exchange) ». [tool-verified: `provisa/core/models.py` `SourceType`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`; `provisa-ui/src/pages/sources/constants.ts`]

**Tables.** Les deux types produisent les mêmes tables canoniques : mêmes noms, mêmes types de colonnes, même ordre. Une union des deux n'exige ni conversion ni renommage ; une colonne pour laquelle un fournisseur n'a pas de valeur est NULL. Les tables sont listées sous le schéma `default`, et chaque ligne commence par le compte dont elle a été lue. Une source offre les tables des ressources qu'elle lit : [tool-verified: `provisa/core/canonical_mail.py` module docstring, `RESOURCE_TABLES`, `tables_for`; `provisa/google_workspace/loader.py` `SCHEMA`]

| Ressource | Tables | Écrites par des instructions |
| --- | --- | --- |
| `mail` | `messages`, `message_recipients`, `folders`, `message_folders`, `threads`, `attachments` | Non |
| `calendar` | `calendars`, `events`, `event_attendees` | `events`, `event_attendees` |
| `tasks` | `task_lists`, `tasks` | `tasks` |

Les colonnes sont listées dans [le schéma canonique du courrier](arch/canonical-mail-schema.md). `events` contient des événements uniques, des maîtres de série avec leur récurrence sous forme de lignes RFC 5545, et des exceptions ; elle ne contient jamais les occurrences développées d'une série. [tool-verified: `provisa/google_workspace/calendar_read.py` module docstring] (REQ-1994)

**Enregistrement.** L'administrateur d'une organisation saisit d'abord le client de l'organisation pour la plateforme sous Admin → Email : un ID client et un secret, plus l'ID de l'annuaire (tenant) ou le domaine pour Microsoft 365. Personne qui ajoute une source ne le voit ni ne le saisit. Une source prend alors l'une de deux formes : [tool-verified: `provisa/core/mail_platforms.py` module docstring; `provisa/microsoft365/__init__.py`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json`]

- **Une boîte aux lettres.** La source désigne une adresse dans `accounts`. Son titulaire clique sur **Connect** dans le formulaire et approuve le client de l'organisation. L'approbation obtenue est conservée dans le magasin d'état de l'organisation, non dans les paramètres de la source. Sur Google Workspace, cette forme énonce aussi `sign_in: google_account`.
- **Les boîtes aux lettres de l'organisation.** La source énonce `mailboxes` comme exactement l'un de `everyone: true`, `group: <address or id>` ou `list: [addresses]`. Elle ne détient aucun identifiant et personne ne la connecte. L'organisation lit avec son propre accès, et seulement une fois que l'administrateur a activé « Read the organization's mailboxes ». Sur Google, cet accès est une clé de compte de service avec délégation à l'échelle du domaine, plus une adresse d'administrateur pour les appels à l'annuaire. Sur Microsoft, c'est l'octroi par identifiants client de l'enregistrement d'application.

[tool-verified: `provisa/google_workspace/settings.py` `parse`, `_mailboxes`; `provisa/microsoft365/settings.py` `parse`; `provisa/core/mail_platforms.py` `organisation_mailboxes`]

Avec la seconde forme, une seule table contient les lignes de toutes les boîtes aux lettres. Un rôle qui peut lire la table lit chaque boîte, sauf si une règle de ligne sur la colonne `account` le limite. [tool-verified: `provisa-ui/src/i18n/locales/en/googleWorkspaceFields.json` `everyMailbox`]

```yaml
sources:
  - id: team-mail
    type: microsoft_365
    mapping:
      resources: [mail, calendar, tasks]
      mailboxes:
        group: support@acme.com
      mail_since: "2026-01-01"
      calendar_check_days: 30        # required when calendar is read
      calendar_send_updates: false
```

**Paramètres.** Chaque module refuse en la nommant une clé qu'il ne lit pas. [tool-verified: `provisa/google_workspace/settings.py` `_KEYS`; `provisa/microsoft365/settings.py` `_KEYS`]

| Clé de `mapping` | Type | Signification |
| --- | --- | --- |
| `resources` | les deux | Lesquelles de `mail`, `calendar`, `tasks` la source lit ; au moins une |
| `accounts` | les deux | L'adresse de la boîte unique (forme à une boîte) |
| `mailboxes` | les deux | `everyone`, `group` ou `list` (forme organisation) |
| `sign_in` | `google_workspace` | `google_account` (forme à une boîte) |
| `mail_since` | les deux | `YYYY-MM-DD` ; rien de reçu avant n'est conservé |
| `mail_content` | `google_workspace` | `full`, ou `headers` pour les seuls en-têtes et libellés (la portée `gmail.metadata` de Gmail). Obligatoire quand le courrier est lu |
| `mail_search` | `google_workspace` | Une recherche telle qu'on la saisit dans la zone de recherche de Gmail |
| `mail_labels` | `google_workspace` | Noms de libellés ; seul le courrier qui les porte tous est lu |
| `mail_include_spam_trash` | `google_workspace` | `false` par défaut |
| `calendar_send_updates` | les deux | Si une écriture prévient les participants d'un événement ; désactivé par défaut |
| `calendar_check_days` | `microsoft_365` | Nombre de jours de part et d'autre d'aujourd'hui que regarde le contrôle des changements ; un entier, 1 ou plus, obligatoire quand l'agenda est lu |

Google refuse une recherche sous `gmail.metadata` : `mail_content: headers` avec `mail_search` ou `mail_since` est donc refusé. [tool-verified: `provisa/google_workspace/settings.py` `_mail`]

**Portées.** Une connexion ne demande que les portées des ressources cochées : [tool-verified: `provisa/google_workspace/settings.py` `MAIL_SCOPES`, `RESOURCE_SCOPES`; `provisa/google_workspace/sign_in.py` `EMAIL_SCOPE`; `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`, `BASE_SCOPES`; `provisa/microsoft365/directory.py`]

| Ressource | Google Workspace | Microsoft 365 (délégué) |
| --- | --- | --- |
| `mail` | `gmail.readonly`, ou `gmail.metadata` pour les seuls en-têtes | `Mail.Read` |
| `calendar` | `calendar.calendarlist.readonly`, `calendar.events` | `Calendars.ReadWrite` |
| `tasks` | `tasks` | `Tasks.ReadWrite` |
| toute source | `userinfo.email` | `offline_access`, `User.Read` |

Pour la forme organisation, Microsoft exige les autorisations d'application `Mail.Read` et `User.Read.All`, plus `Calendars.ReadWrite` et `Tasks.ReadWrite.All` pour les agendas et les tâches. Le compte de service de Google est autorisé pour les portées de `DELEGATED_SCOPES`, qui incluent aussi la lecture des utilisateurs et des membres de groupes de l'annuaire. [tool-verified: `provisa/microsoft365/directory.py`; `provisa/google_workspace/__init__.py` `DELEGATED_SCOPES`; `provisa-ui/src/i18n/locales/en/mailPlatforms.json` `organisationGrant`]

**Rafraîchissement et fraîcheur.** Les types de sonde que prennent ces sources sont `hash` et `none`, et leur chargeur fournit le jeton de changement : [tool-verified: `provisa/events/probes.py` `_MAIL_TYPES`, `probe_capabilities`]

- **Gmail :** un condensat de l'history id courant de chaque boîte, un appel à `users.getProfile` par boîte. [tool-verified: `provisa/google_workspace/loader.py` `mailbox_token`]
- **Courrier Microsoft :** chaque lien delta conservé, suivi pour un élément. [tool-verified: `provisa/microsoft365/loader.py` `unchanged`]
- **Agendas et tâches Google :** l'étiquette et l'heure de dernière modification de chaque agenda ; l'étiquette de chaque liste de tâches, ainsi que l'id et l'heure de mise à jour de chaque tâche. [tool-verified: `provisa/google_workspace/calendar_read.py` `change_token`]
- **Agendas et tâches Microsoft :** les lignes de changement de chaque boîte, sur `calendar_check_days` jours de part et d'autre d'aujourd'hui. [tool-verified: `provisa/microsoft365/loader.py` `_calendar_token`] [inferred: that a line is an event's id and change key, or a task's update time, from REQ-1994]

Un contrôle qui ne peut pas être fait demande une lecture ; il ne rapporte jamais « inchangé ». Les tables d'agenda et de tâches sont lues en entier à chaque rafraîchissement. Les répliques de courrier d'une source Microsoft 365 ou Google Workspace sont conservées sur place : chaque partie d'une boîte est écrite à mesure qu'elle arrive et peut être interrogée aussitôt. [tool-verified: `provisa/core/canonical_mail.py` `RESOURCE_TABLES` comment; `provisa/federation/replica_builds.py` `GROUP_READ_TYPES`; `provisa/federation/mail_units.py`] Une source Google Workspace qui restreint par `mail_search` est relue en entier à chaque rafraîchissement, car seul Google peut dire ce qu'une recherche retient. [tool-verified: `provisa/google_workspace/units.py` module docstring]

**Écritures.** `INSERT`, `UPDATE` et `DELETE` sur `events`, `event_attendees` et `tasks` vont au fournisseur par un chemin d'écriture unique pour les deux types. Un `UPDATE` ou un `DELETE` désigne ses lignes par `provider`, `account` et `id` ; un participant est désigné par `event_id` et `address`. Une occurrence d'une série est désignée par `series_id` et `original_start_at`. Une écriture qui fixe une colonne hors de l'ensemble inscriptible de la table est refusée en la nommant. Google écrit avec `sendUpdates=all` quand `calendar_send_updates` est activé et `sendUpdates=none` quand il est désactivé. Microsoft Graph ne peut pas faire un tel changement en silence : une modification des participants d'une réunion est donc refusée, en nommant le paramètre. [tool-verified: `provisa/executor/writable.py` `API_WRITTEN`; `provisa/core/canonical_mail.py` `WRITE_KEYS`, `WRITABLE_COLUMNS`; `provisa-ui/src/i18n/locales/en/mailResources.json` `sendUpdatesHelp`] [tool-verified: `provisa/google_workspace/calendar_write.py`]

**Exclusions.** Le courrier est en lecture seule ; aucune instruction ne modifie une boîte. `calendars`, `task_lists` et toutes les tables de courrier n'acceptent aucune écriture. Le Spam et la Corbeille de Gmail sont laissés de côté sauf si `mail_include_spam_trash` est activé. L'API Tasks de Google ne conserve que le jour d'échéance d'une tâche et n'a pas de rappel. [tool-verified: `provisa/microsoft365/settings.py` `RESOURCE_SCOPES`; `provisa/core/canonical_mail.py` `WRITABLE_COLUMNS`; `provisa/google_workspace/settings.py` `_mail`] [inferred: the Tasks API limit, from REQ-1994]

### Sources API {: #api-sources }

Enregistrez n'importe quel endpoint HTTP comme table interrogeable. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Type d'API | Découverte | Inférence des colonnes |
| --------- | ----------- | ----------------- |
| `openapi` | Analyse de la spécification OpenAPI (REQ-314, REQ-316) | Primitifs → natifs, objets → JSONB |
| `graphql_remote` | Introspection du schéma (REQ-307, REQ-308) | Primitifs → natifs, objets → JSONB |
| `grpc_remote` | Réflexion serveur (REQ-322, REQ-325) | Primitifs → natifs, objets → JSONB |

Les réponses d'API sont récupérées, mises en cache dans PostgreSQL (TTL configurable) et exposées comme types GraphQL (REQ-309, REQ-318, REQ-327). Les tables mises en cache participent aux requêtes fédérées comme n'importe quelle autre source (REQ-313).

**Règles JSONB** : les colonnes complexes (objets, tableaux) stockées en JSONB ne sont pas filtrables (REQ-119). L'accès aux sous-champs passe par l'extraction `->>` en SQL (REQ-151). Les relations sont déclarées entre tables à l'aide de colonnes de clé étrangère scalaires — les colonnes JSONB brutes ne sont pas des cibles de jointure. Utilisez la promotion JSONB pour convertir des champs imbriqués en colonnes scalaires natives lorsqu'il faut filtrer ou joindre dessus (REQ-119).

#### GitHub et GitLab {: #github-and-gitlab }

GitHub et GitLab sont des sources de marque portées par `graphql_remote` : chacune apparaît dans le formulaire Sources sous son propre nom et s'enregistre comme source GraphQL distante qui consigne sa marque. Le formulaire demande un jeton d'accès personnel et rien d'autre. Le point de terminaison (`https://api.github.com/graphql`, `https://gitlab.com/api/graphql`) et le schéma sont livrés avec Provisa ; le schéma n'est donc pas introspecté à l'enregistrement. Le jeton est vérifié à l'ajout de la source. Ajouter la source n'enregistre aucune table : chaque table qu'offre le schéma est disponible sous l'espace de noms de la source (`gh`, `gl`), et le steward enregistre celles qu'il veut. Un champ que le jeton n'a pas le droit de lire est omis d'une table au moment de son enregistrement. [tool-verified: `provisa/graphql_remote/brands.py` `BRANDS`] (REQ-1923)

#### Stripe (REQ-1923) {: #stripe }

Stripe est une source de marque portée par `openapi`, bâtie comme GitHub et GitLab le sont sur `graphql_remote`. Elle apparaît dans le formulaire Sources sous le nom Stripe, sous Enterprise. Elle s'enregistre comme source OpenAPI dont la ligne conserve la marque dans `federation_hints` et dont le chemin de spécification est `brand:stripe`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_CARRIER`; `provisa/openapi/brands.py` `BRANDS`, `SPEC_SCHEME`; `provisa/api/admin/openapi_router.py` `_branded`]

**Ce qu'elle atteint.** L'API REST de Stripe, par la spécification OpenAPI que Stripe publie (`stripe/openapi`, `spec3.json`). La spécification est livrée avec Provisa, épinglée et compressée en gzip, et n'est pas récupérée à l'enregistrement. Celle qui est livrée correspond à la version d'API `2026-09-30.endive`, avec 425 chemins. Chaque appel porte `Stripe-Version` fixé à cette version : chaque réponse a donc la forme que décrit la spécification, quelle que soit la version par défaut du compte. Les champs que Stripe renvoie mais que la spécification omet sont ajoutés depuis `stripe.additions.json`. [tool-verified: `provisa/openapi/brands.py` `brand_spec`, `Brand.headers`; `provisa/openapi/_known_specs/`; version and path count read with the project interpreter]

Comme pour toute source OpenAPI, chaque opération `GET` est une table proposée et chaque autre opération est une commande proposée. Ajouter la source n'en enregistre aucune : le steward enregistre celles qu'il veut. [tool-verified: `provisa/api/admin/openapi_router.py` `register_openapi_source`; `provisa/openapi/brands.py` module docstring]

**Authentification.** Le formulaire ne demande qu'une chose, un jeton d'accès (`BRAND_AUTH` : `token`). Utilisez une clé d'API Stripe secrète ou restreinte. Le jeton est envoyé comme jeton bearer, et vérifié une fois par `GET /v1/balance` avant l'ajout de la source. Un jeton rejeté fait échouer l'enregistrement avec `openapi.credential_rejected` ; un jeton absent, avec `openapi.credential_required`. [tool-verified: `provisa-ui/src/pages/sources/constants.ts` `BRAND_AUTH`; `provisa/openapi/brands.py` `Brand.auth`, `verify_path`; `provisa/api/admin/openapi_router.py` `_branded`] [inferred: that a restricted key works, provided it may read `/v1/balance`]

**Rafraîchissement et fraîcheur.** Une table Stripe est une réplique, comme toute table OpenAPI. Ses réponses en cache durent le `cache_ttl` de la source, qu'une requête d'enregistrement fixe à 300 secondes sauf indication contraire. Les types de sonde OpenAPI s'appliquent. [tool-verified: `provisa/api/admin/openapi_router.py` `OpenAPIRegisterRequest.cache_ttl`; `provisa/events/probes.py` `_HTTP_API_TYPES`] REQ-1923 prévoit qu'un préréglage nomme le paramètre qu'un rafraîchissement utilise pour ne récupérer que ce qui a changé. La marque Stripe n'en nomme aucun : chaque rafraîchissement lit donc ses tables en entier. [tool-verified: `provisa/openapi/brands.py` `Brand` has no such field]

**Exclusions.** L'intégration de paiement dépréciée de Stripe, les jetons et Sources, est laissée hors de la spécification livrée. La liste se trouve sous [Exclusions sur une source construite à partir d'une spécification](#exclusions-on-a-specification-built-source). Les `exclusions` propres à un opérateur s'ajoutent à celles de la marque. [tool-verified: `provisa/openapi/_known_specs/stripe.exclusions.json`; `provisa/openapi/brands.py` `brand_spec`] (REQ-1957)

`stripe` figure aussi dans `SourceType` parmi les types `wrappers` Supabase factices. Une source Stripe ajoutée par le formulaire est une source `openapi`, non ce type factice. [tool-verified: `provisa/core/models.py` `SourceType`]

#### Exclusions sur une source construite à partir d'une spécification {: #exclusions-on-a-specification-built-source }

Une spécification OpenAPI publiée décrit tout ce que l'API distante accepte, y compris ce que son éditeur a déprécié et ce qu'un déploiement ne doit jamais envoyer. Une source OpenAPI peut porter des exclusions : des éléments de la spécification que la source n'offre pas. (REQ-1957)

| Exclu | Désigné par | Effet |
|---|---|---|
| Une opération | `method` et `path` | Non listée pour l'enregistrement comme table ou comme commande |
| Un argument de requête | `name`, sur une opération (`method` et `path`) ou, sans ni l'un ni l'autre, sur toute opération qui le possède | Absent des arguments de l'opération, non affiché, jamais envoyé |

Les exclusions se définissent sous `exclusions` dans le `mapping` de la source :

```yaml
sources:
  - id: shop-api
    type: openapi
    path: https://shop.example.com/openapi.json
    mapping:
      exclusions:
        operations:
          - {method: POST, path: /v1/tokens}
        arguments:
          - {name: source, method: POST, path: /v1/customers}
          - {name: legacy_token}          # on every operation that has it
```

Elles sont appliquées à la spécification au moment de son chargement : la liste d'enregistrement, les commandes générées et la requête envoyée voient donc toutes une spécification dans laquelle les éléments exclus n'existent pas. Une propriété du corps et un paramètre de requête, d'en-tête ou de cookie peuvent être exclus ; un paramètre de chemin ne le peut pas, car il fait partie de l'adresse de l'opération.

Une exclusion qui nomme une opération ou un argument que la spécification n'a pas fait échouer le chargement de la spécification de cette source et nomme chaque entrée périmée : une liste qui ne correspond plus à une spécification plus récente ne passe donc pas inaperçue.

Une source de marque livre ses propres exclusions dans le cadre de sa curation, et celles d'un opérateur s'y ajoutent. La source Stripe exclut l'intégration de paiement dépréciée de Stripe fondée sur les jetons et les sources : créer un débit, les API Tokens et Sources, ajouter une carte ou un compte bancaire à un client à partir d'un jeton, ainsi que les arguments `source`, `card`, `bank_account` et `default_card` des écritures de clients et de paiements de factures. Les paiements passent par les Payment Intents, les Payment Methods et les Setup Intents, que la source offre. La lecture et la suppression des sources de paiement qu'un compte possède déjà restent proposées.


### GovData

Données ouvertes du gouvernement américain. L'accès est partitionné par regroupement thématique. [tool-verified: `provisa/core/models.py` lines 543–609]

Une source reçoit un ou plusieurs thèmes, et chaque thème apporte les schémas d'adaptateur listés ci-dessous. Un schéma peut appartenir à plusieurs thèmes (`census`). Les schémas `ref` et `geo` sont des schémas de liaison : données de référence et géographie par lesquelles les tables de chaque thème se joignent ; toute source les sert donc, quels que soient les thèmes qu'elle reçoit. La liste est énoncée une seule fois sur le serveur, et un test la maintient égale aux schémas que sert le bundle d'adaptateur épinglé. [tool-verified: `provisa/core/models.py` `GOVDATA_SUBJECT_SCHEMAS`, `GOVDATA_LINKER_SCHEMAS`; `tests/unit/test_govdata_subject_schemas.py`]

| Thème | Schémas exposés |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents`, `cftc` |
| `ECONOMY` | `econ`, `econ_reference`, `ag`, `banking`, `fiscal`, `housing`, `transport` |
| `EDUCATION` | `census`, `edu`, `research` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime`, `disasters` |
| `ENVIRONMENT` | `lands`, `environment` |
| `WEATHER` | `weather` |
| `ENERGY` | `energy` |
| `GOVERNMENT` | `fedregister`, `fec`, `officials`, `law` |
| `DEMOGRAPHICS` | `census` |
| toute source | `ref`, `geo` |

`ALL` est le raccourci d'un abonnement pour tous les thèmes ; ce n'est pas un choix du formulaire Sources.


```yaml
sources:

  - id: federal-commerce
    type: govdata
    subject: COMMERCE
    domain_id: federal-analytics
    description: U.S. commerce and securities data
```

| Champ | Requis | Par défaut | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Oui | — | Identifiant unique |
| `subject` | Oui | — | L'une des valeurs de thème ci-dessus |
| `domain_id` | Oui | — | Domaine auquel appartient cette source |
| `description` | Non | `""` | Description lisible |

Une source AskAmerica créée dans l'administration prend un seul identifiant, la clé d'API. Provisa présente la clé à l'API d'AskAmerica et reçoit les identifiants de stockage éphémères, en lecture seule, avec lesquels les données sont lues ; la clé y est refusée par son nom si elle est inconnue ou révoquée. [tool-verified: `provisa/federation/askamerica.py`]

La source est alors une source à protocole Postgres, comme celles de SharePoint, Salesforce et de l'inventaire cloud : Provisa lance pour elle le serveur `pgwire-govdata` fourni, avec la clé et ces identifiants dans l'environnement du serveur, et chaque moteur atteint ce serveur comme il atteint celui d'une source sœur. DuckDB, Postgres et ClickHouse l'attachent et lisent ses tables sur place ; une table AskAmerica se joint donc aux tables d'autres sources, se place sous des vues et est livrée comme n'importe quelle autre. Trino n'a pas de connecteur pour elle et lit un réplica déposé à travers le serveur : une copie de la table conservée dans le magasin de matérialisation, aussi fraîche que son dernier rafraîchissement selon les réglages de cache et de réplication de la table, et non lue au moment de la requête. Une table est lue depuis le schéma d'adaptateur qu'elle nomme (`sec`, `econ`, etc.). [tool-verified: `provisa/federation/pgwire_replica.py`, `provisa/federation/connector_duckdb.py`, `provisa/federation/clickhouse_connectors.py`]

Le serveur monte ses schémas depuis le stockage d'objets avant d'accepter une connexion, ce qui prend des minutes au premier démarrage. Il est lancé quand la source est enregistrée ; tant qu'il n'est pas prêt, le formulaire Register Table indique que la source démarre encore. [tool-verified: `provisa/federation/pgwire_replica.py` `start_when_saved`]

### Où vit le mot de passe d'une source {: #where-a-sources-password-lives }

Le mot de passe d'une source n'est jamais stocké à côté du reste de ses paramètres de
connexion. La ligne `sources` du plan de contrôle porte une colonne `password_ref` qui contient ce qui
a été saisi pour le mot de passe : la valeur elle-même, ou une référence telle que `${env:PG_PASSWORD}`
ou `${secret:SNOWFLAKE_KEY}`. Elle est résolue au moment où la source est composée, à l'intérieur de
l'organisation pour le compte de laquelle s'exécute la requête. [tool-verified:
`provisa/core/schema_org.py`, `provisa/core/repositories/source.py`]

Une valeur sans `${…}` est utilisée telle quelle. `${env:VAR}` lit l'environnement de processus du
déploiement et ne nécessite aucune liaison. `${secret:NAME}` nomme un secret possédé par une
organisation, donc il ne se résout qu'à l'intérieur des propres opérations de cette organisation : les
points d'entrée d'introspection d'administration et le terminal de requête que chaque surface atteint
établissent cette liaison. [tool-verified: `provisa/core/secrets.py` `resolve_secrets`;
`provisa/pgwire/_pipeline.py` `_execute_plan`]

Le champ contient ce que vous y saisissez, quelle que soit la façon dont la source a été enregistrée :

- **Depuis la configuration.** Le fichier fait foi : Provisa copie la valeur ou la référence dans
  `password_ref` telle quelle.
- **Depuis le formulaire Sources.** Ce qui est saisi dans le champ mot de passe est stocké tel quel,
  valeur ou référence. Provisa n'écrit rien dans le coffre-fort de l'organisation à ce titre et
  n'invente aucun nom de secret. Pour garder un identifiant hors de la ligne du plan de contrôle,
  créez vous-même le secret (voir [Secrets](secrets.md)) et saisissez `${secret:NAME}` dans le champ.
  [tool-verified: `provisa/api/admin/schema_mutation.py` `create_source`]

Une valeur saisie comme valeur est stockée telle quelle dans la base du plan de contrôle. Ressaisir le
mot de passe remplace le texte du champ. Supprimer la source retire sa ligne et rien du coffre-fort : un
secret que vous avez créé vous appartient. [tool-verified: `provisa/api/admin/schema_mutation.py`
`delete_source`; `tests/unit/test_no_product_written_secrets.py`]


`password_ref` ne voyage pas entre environnements (REQ-1491). Une branche ou un environnement
copié fournit ses propres valeurs de connexion, et le coffre-fort que nomme une référence appartient à
l'environnement qui l'a fournie. [tool-verified: `provisa/core/env_classes.py` `BINDING_COLUMNS`]

### Profileur de données {: #data-profiler }

Un profileur de données est une source qui détient un nom, un calendrier et des valeurs par défaut d'exécution, et rien d'autre. Il n'est jamais introspecté. Des tables d'autres sources lui sont ajoutées, et chaque exécution les mesure et écrit ses lignes dans le plan de contrôle de l'organisation, où elles se lisent comme les tables de n'importe quelle autre source. Pour créer un profileur et lui ajouter des tables, voir [Données de test](test-data.md). [tool-verified: `provisa/core/models.py` `SourceType.data_profiler`; `provisa/federation/strategy.py` `_MATERIALIZE_ONLY`] (REQ-1934)


### Vérificateurs de qualité des données (REQ-1443) {: #data-quality-checkers-req-1443 }

Un vérificateur de qualité des données est un type de source, non un sous-système. Sa sortie de balayage est une donnée : un résultat de contrôle est une observation, il atterrit donc par le chemin ordinaire des sources et hérite de la cadence, de la fraîcheur, des événements, de la traçabilité, de la gouvernance, du RLS, de la grille et de l'export comme toute autre source. [tool-verified: `provisa/core/models.py` lines 110–116 `SourceType.soda`, `SourceType.great_expectations`; `provisa/events/source_loader.py` `make_dq_loader`]

Deux sont pris en charge, et le choix relève autant de la licence que de la fonctionnalité.

| Type de source | Dialecte de contrat | Extra | Licence | Plan cloud hébergé |
| ------------ | ----------------- | ------- | --------- | -------------------- |
| `soda` | YAML de contrat Soda | `pip install .[soda]` (`soda-postgres`) | Elastic License 2.0 | Refusé — voir ci-dessous |
| `great_expectations` | JSON de suite d'attentes | `pip install .[gx]` (`great-expectations[postgresql]`) | Apache 2.0 | Autorisé |

La licence Elastic License 2.0 interdit de fournir le logiciel à des tiers comme service hébergé ou managé, et exécuter Soda dans le plan SaaS pour le compte d'un locataire est exactement cela. `config/capabilities.yaml` porte cette scission sous la forme `cloud_eligible: false` sur l'option `soda`, et le plan hébergé lit ce drapeau. Un déploiement hébergé qui souhaite Soda atteint un endpoint Soda fourni par l'exploitant, que celui-ci exécute lui-même. [tool-verified: `config/capabilities.yaml` lines 197–203]

Provisa n'intègre ni ne lie quoi que ce soit. Le balayage s'exécute dans un interpréteur enfant (`python -m provisa.dq.worker`), seul endroit où `soda_core` ou `great_expectations` est importé, de sorte qu'un vérificateur à source disponible n'atteint jamais le processus serveur et qu'un plantage du vérificateur tue un sous-processus plutôt que la boucle d'événements. [tool-verified: `provisa/dq/runner.py` `build_command`, `run_contract`]

**La source pointe vers l'endpoint pgwire de Provisa lui-même.** C'est ce qui permet à un unique pilote postgres de contrôler une table adossée à Snowflake ou à Iceberg : le vérificateur balaie la vue fédérée, non le système sous-jacent. Parce que la politique s'applique à cette connexion, l'identité de balayage est déclarée plutôt qu'héritée — un jeu de lignes filtré ne doit jamais produire un contrôle silencieusement réussi.

```yaml
sources:

  - id: dq
    type: soda
    domain_id: sales-analytics
    description: Soda contract scans over the governed estate
    mapping:
      host: localhost
      port: 5439          # Provisa's pgwire endpoint
      database: provisa
      user: dq_scanner    # the scan identity, declared explicitly
      password: ${env:PROVISA_DQ_PASSWORD}
```

**Une table de résultats par contrat, et le contrat constitue tout l'enregistrement.** La table porte `dq_contract` — le texte du contrat tel quel — et rien d'autre quant à sa forme. Colonnes, filigrane et promotions en sont tous dérivés. [tool-verified: `provisa/dq/registration.py` `derive_checker_table`]

```yaml
tables:

  - source_id: dq
    schema_name: quality
    table_name: orders_scan
    domain_id: sales-analytics
    change_signal: ttl_probe
    cache_ttl: 3600
    columns:
      - name: scan_id          # declared only to carry visible_to; replaced at parse
        visible_to: [analyst, admin]
    dq_contract: |
      dataset: provisa/sales/orders
      columns:
        - name: customer_id
          checks:
            - missing:
                threshold:
                  metric: percent
                  must_be_less_than: 1
      checks:
        - row_count:
            must_be_greater_than: 0
```

Ce que l'enregistrement dérive de ce texte :

- **Traçabilité.** Le contrat nomme déjà son jeu de données cible, si bien que l'enregistrement l'analyse comme `extract_inputs` analyse le SQL (REQ-939) et le résout vers la table gouvernée. Une seule définition, aucune seconde copie susceptible de dériver. Un contrat nommant un jeu de données non gouverné échoue bruyamment à l'enregistrement plutôt que de déposer des lignes que personne n'a demandées.
- **Colonnes.** L'enveloppe de résultat appartient au vérificateur, non à l'exploitant — 16 colonnes livrées, de `scan_id` à `diagnostics`. Les colonnes déclarées ne sont lues que pour leur `visible_to`, qui doit être unanime, puis elles sont remplacées. [tool-verified: `provisa/dq/results.py` `_ENVELOPE`, `results_columns`]
- **Filigrane.** `scan_time` devient le filigrane, ce qui fait du dépôt un ajout (REQ-982). L'historique des balayages s'accumule sans sous-système d'historique.
- **Promotions.** `freshness_max_timestamp` et `dataset_rows_tested` sont promus hors du jsonb `diagnostics` comme colonnes typées (REQ-119). Ajoutez-en d'autres comme vous le feriez sur n'importe quelle autre colonne jsonb. [tool-verified: `provisa/dq/results.py` `DQ_PROMOTIONS`]

Le calendrier n'introduit aucun champ nouveau. `change_signal` plus `cache_ttl` donnent la cadence d'interrogation ; `mv_debounce_quiet` et `mv_debounce_max_delay` regroupent une rafale amont en un seul balayage (REQ-963) ; un grain calendaire le rend périodique (REQ-962) ; `expected_events` retient le balayage jusqu'à ce que ses entrées soient fraîches sur toute la fenêtre (REQ-961). La boucle d'interrogation est l'ordonnanceur des balayages.

`outcome` vaut `pass`, `fail`, `warn`, `error` ou `skipped`. Aucune de ces valeurs n'est un verdict — l'application, si elle est souhaitée, est une déclaration distincte, plus tard : un contrôle préalable ou une MV sur les résultats déposés. Parce qu'une observation déposée ne porte aucune obligation de déterminisme (REQ-964), des contrôles non déterministes sont admissibles ici alors qu'ils ne pourraient jamais tenir sur une barrière de contrôle préalable — score d'anomalie, variation sur fenêtre glissante, fraîcheur par rapport à maintenant.

Le contrat est rédigé dans l'interface, dans le panneau de qualité des données de la surface d'édition de table, et le texte brut du contrat y fait toujours foi. Une exécution à blanc exécute le contrat contre la table vivante et montre les résultats sans les déposer — c'est ainsi que l'on attrape un contrat dont le nom de jeu de données s'est résolu vers un endroit inattendu et qui ne déposerait sinon que des lignes en réussite.

---

## Connecteurs personnalisés (REQ-1177) {: #custom-connectors-req-1177 }

Les moteurs de fédération natifs — Postgres, DuckDB et ClickHouse — gagnent l'accès à un nouveau type de source lorsqu'un exploitant déclare un connecteur pour ce type dans `config/custom_connectors.yaml`. Aucun code n'est requis. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

L'extensibilité des connecteurs elle-même est antérieure. Le moteur Trino est extensible de longue date à son propre niveau — un connecteur JDBC générique paramétré par type de source, un corps de catalogue `.properties` par type, et les greffons de connecteurs Trino personnalisés de Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 apporte cette même extensibilité pilotée par configuration aux deux moteurs natifs sans cluster, qui portaient auparavant un jeu de connecteurs figé.

La configuration est livrée vide. Les connecteurs intégrés couvrent la portée prête à l'emploi ; tout ce que contient ce fichier est rédigé par l'exploitant. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Définissez `PROVISA_CUSTOM_CONNECTORS` pour pointer vers un autre chemin (utile pour les tests).

### Genres de descripteurs {: #descriptor-kinds }

| Moteur | Genre | Mécanisme | Ce que fournit le descripteur |
| --- | --- | --- | --- |
| `postgres` | `pg_fdw` | SQL/MED (norme ISO) | `extension`, `server_options`, `user_mapping`, `supports_import`, `table_options`, `remote_schema` |
| `duckdb` | `duckdb_attach` | INSTALL/LOAD + ATTACH | `extension`, `probe_symbol`, `attach_template`, `remote_schema` |
| `duckdb` | `duckdb_scan` | INSTALL/LOAD + vue de scanner | `extension`, `probe_symbol`, `scan_template` |
| `clickhouse` | `clickhouse_database` | `CREATE DATABASE ENGINE=…` (expose automatiquement toutes les tables distantes) | `ch_engine`, `engine_template` |
| `clickhouse` | `clickhouse_table` | `CREATE TABLE ENGINE=…` par table (colonnes issues du registre) | `ch_engine`, `engine_template` (peut porter `{table}`) |
| `clickhouse` | `clickhouse_scan` | `CREATE TABLE ENGINE=…`, ClickHouse infère le schéma | `ch_engine`, `engine_template` |

**Postgres est générique.** SQL/MED est une norme ISO, si bien que tout FDW conforme partage la même forme de DDL : `CREATE SERVER … FOREIGN DATA WRAPPER <fdw> OPTIONS(…)`, un `CREATE USER MAPPING` facultatif, puis soit `IMPORT FOREIGN SCHEMA` (quand `supports_import: true`), soit un `CREATE FOREIGN TABLE` explicite par table (quand `false`). Un descripteur `pg_fdw` ne fournit que la variance propre à chaque FDW — nom d'extension, clés d'options du serveur, clés du mappage utilisateur, drapeau d'import, options de table. Tout FDW conforme à la norme est donc pilotable depuis la seule configuration. [tool-verified: `provisa/federation/custom_connectors.py` `GenericPgFdwConnector.details` lines 98–125]

**DuckDB prend en charge deux mécanismes.** Une extension exposant un catalogue via ATTACH utilise `duckdb_attach` ; une extension exposant une fonction de table en lecture utilise `duckdb_scan`. Une extension ne correspondant à aucun de ces schémas n'est pas prise en charge. [tool-verified: `provisa/federation/custom_connectors.py` `GenericDuckDbAttachConnector`, `GenericDuckDbScanConnector`]

**ClickHouse prend en charge trois mécanismes**, un par forme de moteur d'intégration : un moteur DATABASE relationnel qui expose automatiquement toutes les tables distantes (`clickhouse_database`, par exemple Redis/MySQL), un moteur par table dont le registre fournit les colonnes (`clickhouse_table`, par exemple le pont JDBC/ODBC — le `engine_template` peut porter un caractère de remplacement `{table}` que le runtime lie), et un moteur fichier/lac/URL dont ClickHouse infère le schéma (`clickhouse_scan`, par exemple HDFS/URL). SQLite (moteur DATABASE, fichier, sans serveur) et Hudi (lakehouse, sans copie) sont livrés d'origine. [tool-verified: `provisa/federation/custom_connectors.py` `GenericClickHouseDatabaseConnector`, `GenericClickHouseTableConnector`, `GenericClickHouseScanConnector`; `provisa/federation/clickhouse_connectors.py` `ClickHouseSqliteConnector`, `ClickHouseHudiConnector`] (REQ-1178)

Une valeur de `kind` inconnue échoue bruyamment au démarrage — une faute de frappe dans un descripteur ne doit pas laisser silencieusement un type de source hors d'atteinte. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors` lines 178–197]

### Contrôle par sondage {: #probe-gating }

La disponibilité est vérifiée au moment de l'attachement contre le catalogue de découverte standard de chaque moteur :

- **Postgres** — vérifie `pg_extension`, puis `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — exécute `INSTALL`/`LOAD` et vérifie `duckdb_functions()` pour le `probe_symbol` déclaré. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — vérifie `system.table_engines` pour le `ch_engine` déclaré ; son absence de la build échoue bruyamment. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Une extension déclarée mais non installable échoue bruyamment. Aucun saut silencieux, aucun repli. Un connecteur dont le sondage échoue n'est simplement pas actif pour ce déploiement.

### Variables de gabarit {: #template-variables }

Chaque valeur de `server_options`, chaque valeur de `user_mapping`, chaque `attach_template` et chaque `scan_template` peut employer des caractères de remplacement `{field}`. Champs disponibles : [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, plus toute clé issue de `federation_hints`. Les gabarits d'attachement DuckDB reçoivent également `{alias}` — l'alias de catalogue interne que Provisa attribue à la base attachée.

Un gabarit référençant un champ inconnu échoue bruyamment au moment de l'attachement, faisant apparaître une discordance descripteur/source avant qu'un DDL cassé n'atteigne le moteur.

### Exemples {: #examples }

**Postgres — MongoDB via `mongo_fdw` (sans import de schéma ; colonnes fournies par table)**

```yaml
# config/custom_connectors.yaml
connectors:
  - engine: postgres
    source_type: mongodb
    kind: pg_fdw
    extension: mongo_fdw
    mechanism: attach_r
    server_options:
      address: "{host}"
      port: "{port}"
    user_mapping:
      username: "{username}"
      password: "{password}"
    supports_import: false
    table_options:
      database: "{database}"
      collection: "{table_name}"
```

**DuckDB — fichiers Excel via `read_xlsx` (fonction de table de balayage)**

```yaml
  - engine: duckdb
    source_type: xlsx
    kind: duckdb_scan
    extension: excel
    install_from_community: false
    probe_symbol: read_xlsx
    scan_template: "read_xlsx('{path}')"
```

[tool-verified: `config/custom_connectors.yaml` commented examples, lines 26–50]

Avec l'un ou l'autre descripteur en place, l'enregistrement d'une source portant le `source_type` déclaré passe par le connecteur personnalisé, sous réserve d'un sondage réussi. Aucun autre changement de configuration n'est nécessaire.

---

## Entrepôts de données comme sources nommées {: #warehouses-as-named-sources }

Snowflake, Databricks et ClickHouse peuvent être enregistrés comme sources nommées indépendamment du moteur de fédération actif. [tool-verified: `executor/drivers/snowflake.py` (REQ-988), `executor/drivers/databricks.py` (REQ-987), `executor/drivers/clickhouse.py` (REQ-986)]

Une fois enregistré, Provisa lit l'entrepôt via le DirectDriver de la source et dépose un réplica dans le magasin de matérialisation du moteur actif. La requête s'exécute ensuite contre ce réplica. Cela diffère du chemin traditionnel « capable en direct » (asyncpg, aiomysql) où le moteur est entièrement contourné — ici le moteur exécute toujours la requête, mais contre un réplica local plutôt qu'à travers le réseau vers l'entrepôt à chaque demande.

Les lectures sont nativement Arrow lorsque l'entrepôt le permet : Databricks utilise Cloud Fetch, Snowflake utilise `fetch_arrow_table`, et ClickHouse utilise l'interface HTTP colonnaire native.

Les paramètres de connexion étendus que les champs standards `host`/`port`/`username`/`password` ne peuvent pas porter vont dans `federation_hints` :

```yaml
sources:
  - id: my-databricks
    type: databricks
    host: my-workspace.azuredatabricks.net
    password: ${env:DATABRICKS_TOKEN}
    federation_hints:
      http_path: /sql/1.0/warehouses/xxxx   # required — the SQL Warehouse connection detail

  - id: my-snowflake
    type: snowflake
    host: org.snowflakecomputing.com
    username: svc_provisa
    password: ${env:SNOWFLAKE_PASSWORD}
    federation_hints:
      account: myorg-myaccount    # required — Snowflake account identifier
      warehouse: COMPUTE_WH       # optional — virtual warehouse to use
      role: PROVISA_ROLE          # optional — Snowflake role

  - id: my-clickhouse
    type: clickhouse
    host: ch.example.com
    port: 8123
    database: analytics
    username: default
    password: ${env:CLICKHOUSE_PASSWORD}
    federation_hints:
      secure: "true"              # optional — enables TLS on the HTTP interface
```

L'enregistrement comme source nommée est indépendant du choix du même entrepôt comme moteur de fédération. Une source Snowflake sur un moteur DuckDB dépose un réplica dans DuckDB, non dans Snowflake.

Les données cloud d'objets ou de lac (fichiers parquet, csv, iceberg, delta_lake sur S3 / GCS / R2) constituent un type de source distinct qui s'attache sur place lorsque le moteur actif dispose d'un connecteur ATTACH pour ce type. Aucun réplica n'est déposé — le moteur balaie directement le stockage objet. Les identifiants de ces sources vont eux aussi dans `federation_hints` :

```yaml
sources:
  - id: r2-events
    type: parquet
    path: s3://my-bucket/events/2026/*.parquet
    federation_hints:
      access_key_id: ${env:R2_ACCESS_KEY}
      secret_access_key: ${env:R2_SECRET}
      account_id: ${env:R2_ACCOUNT_ID}     # Cloudflare R2 account (S3-compatible)
```

---

## Champs de configuration des sources {: #source-configuration-fields }

Toutes les sources partagent un jeu commun de champs. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Champ | Requis | Par défaut | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Oui | — | Identifiant unique ; alphanumérique avec tirets/soulignés |
| `type` | Oui | — | Type de source (voir les tableaux ci-dessus) |
| `host` | Non | `""` | Nom d'hôte ou IP |
| `port` | Non | `0` | Numéro de port |
| `database` | Non | `""` | Nom de la base |
| `username` | Non | `""` | Nom d'utilisateur |
| `password` | Non | `""` | Mot de passe ; utilisez `${env:VAR}` ou `${secret:NAME}` plutôt qu'un littéral (voir ci-dessous) |
| `path` | Non | `null` | Chemin de fichier ou URI cloud pour les sources fondées sur des fichiers et les sources objet/lac |
| `base_url` | Non | `null` | URL de base pour les sources OpenAPI |
| `pool_min` | Non | `1` | Taille minimale du pool de connexions (REQ-052) |
| `pool_max` | Non | `5` | Taille maximale du pool de connexions (REQ-052) |
| `use_pgbouncer` | Non | `false` | Router les connexions à travers PgBouncer (REQ-053) |
| `pgbouncer_port` | Non | `6432` | Port PgBouncer (REQ-053) |
| `cache_enabled` | Non | `true` | Activer la mise en cache des réponses d'API |
| `cache_ttl` | Non | `null` | TTL du cache en secondes ; hérite du défaut global s'il vaut null |
| `cache_catalog` | Non | `null` | Catalogue fédéré pour le cache d'API ; par défaut, le catalogue propre à la source |
| `cache_schema` | Non | `api_cache` | Schéma au sein du catalogue de cache |
| `naming_convention` | Non | `null` | Surcharger la convention de nommage globale pour cette source (REQ-194) |
| `federation_hints` | Non | `{}` | Propriétés de session transmises au moteur de fédération, et paramètres de connexion étendus pour les sources d'entrepôt (REQ-278, REQ-281) |
| `mapping` | Non | `{}` | Réglages de connecteur propres au type pour les sources NoSQL et SaaS (par exemple `auth_type` SharePoint, `auth_type` Salesforce, `use_token` Splunk) (REQ-251) |
| `allowed_domains` | Non | `[]` | Restreindre la source à des domaines précis ; vide = sans restriction |
| `description` | Non | `""` | Description lisible |

---

## Sources Kafka {: #kafka-sources }

Les topics Kafka se configurent séparément sous `kafka_sources`, indexés par l'`id` de source d'une source `kafka` enregistrée. [tool-verified: `config/provisa.yaml` lines 138–151] (REQ-147)

```yaml
kafka_sources:

  - id: kafka-support
    topics:

      - id: tickets
        topic: support.tickets
        domain_id: sales-analytics
        description: "Inbound support tickets"
        default_window: 1h
        columns:

          - name: id
          - name: subject
          - name: status
          - name: created_at
```

| Champ | Description |
| ------- | ------------- |
| `id` | Doit correspondre à l'`id` d'une source de `type: kafka` |
| `topics[].id` | Nom logique de ce topic au sein de Provisa |
| `topics[].topic` | Nom du topic Kafka |
| `topics[].domain_id` | Domaine auquel appartient ce topic |
| `topics[].description` | Description lisible |
| `topics[].default_window` | Fenêtre temporelle par défaut pour les requêtes fenêtrées (par exemple `1h`) (REQ-148) |
| `topics[].columns` | Définitions de colonnes pour le schéma du topic (REQ-150) |

---

## Visibilité des colonnes {: #column-visibility }

Le champ `visible_to` de chaque colonne est une liste d'identifiants de rôles pouvant voir cette colonne. [tool-verified: `provisa/core/models.py` `Column` class line 248 ; `config/provisa.yaml` lines 39–51]

```yaml
columns:

  - name: email
    visible_to: [admin]        # only admin role sees this column

  - name: region
    visible_to: [admin, analyst]  # both roles see this column
```

Les colonnes absentes de la liste `visible_to` d'un rôle n'apparaissent pas dans le schéma GraphQL de ce rôle et ne peuvent être ni interrogées ni référencées dans des filtres (REQ-039).

---

## Relations {: #relationships }

Les relations connectent deux tables enregistrées et apparaissent comme des champs imbriqués en GraphQL. [tool-verified: `provisa/core/models.py` `Relationship` class lines 323–343 ; `config/provisa.yaml` lines 103–110] (REQ-019)

```yaml
relationships:

  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

| Champ | Requis | Description |
| ------- | ---------- | ------------- |
| `id` | Oui | Identifiant unique de cette relation |
| `source_table_id` | Oui | Table qui porte la clé étrangère |
| `target_table_id` | Oui | Table référencée ; vide pour les relations calculées |
| `source_column` | Oui | Colonne de la table source |
| `target_column` | Oui | Colonne de la table cible ; vide pour les relations calculées |
| `cardinality` | Oui | `many-to-one` ou `one-to-many` (REQ-019) |
| `materialize` | Non | Créer automatiquement une vue matérialisée pour les jointures inter-sources (REQ-158). Sur une arête adossée à une jonction, la vue couvre le parcours à deux sauts, non une jointure directe (REQ-1586) |
| `refresh_interval` | Non | Intervalle de rafraîchissement de la MV en secondes (par défaut : 300) |
| `target_function_name` | Non | Nom de la fonction de base de données pour les relations calculées |
| `function_arg` | Non | Quel argument de la fonction reçoit la valeur de la colonne source |
| `alias` | Non | Type de relation lisible (par exemple `WORKS_FOR`) |
| `graphql_alias` | Non | Nomme le champ SDL que cette relation expose sur le type parent. En son absence, le nom est dérivé du `field_name` de la table cible et de la cardinalité de la relation. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Non | Quand `true`, exclure cette relation des arêtes du graphe Cypher |
| `source_json_key` | Non | Extraire cette clé de la colonne source comme objet JSON avant le JOIN |
| `via_table` | Non | Nom de table enregistrée de la jonction que traverse cette arête. La renseigner rend l'arête adossée à une jonction ; la laisser vide en fait une arête de clé étrangère (REQ-1586) |
| `via_source_column` | Non | Colonne de jonction appariée à `source_column`. Séparée par des virgules et positionnelle pour une clé composite |
| `via_target_column` | Non | Colonne de jonction appariée à `target_column` |
| `via_type_column` | Non | Colonne discriminante, quand une même jonction porte plusieurs types de relation |
| `via_type_value` | Non | La valeur de discriminant à laquelle cette arête est fixée |
| `via_label_source` | Non | Quelle désignation nomme le type Cypher : `column` (la valeur du discriminant), `table` (le nom de la table de jonction) ou `fixed` (l'alias déclaré). Toutes sont mises en UPPER_SNAKE_CASE |

### Relations adossées à une jonction {: #junction-backed-relationships }

Une table associative peut être déclarée comme relation Cypher de première classe plutôt que comme nœud, de sorte que ses propres colonnes deviennent les attributs de cette relation : (REQ-1586)

```yaml
relationships:

  - id: pets-bonded-pair
    source_table_id: pets
    target_table_id: pets
    source_column: id
    target_column: id
    cardinality: one-to-many
    via_table: pet_companions
    via_source_column: pet_id
    via_target_column: companion_pet_id
    via_type_column: relation_type
    via_type_value: bonded pair
    via_label_source: column
```

La jonction est une table enregistrée comme une autre et doit l'être avant qu'une relation puisse la nommer. Déclarez-la une fois par valeur de discriminant : trois lignes sur `pet_companions` produisent `BONDED_PAIR`, `LITTERMATE` et `SHARES_ENCLOSURE` comme trois types Cypher distincts, chacun portant les colonnes restantes de la ligne de jonction comme propriétés d'arête. La configuration de démo livrée déclare exactement cela.

Une arête de jonction est une relation Cypher, pas un champ de jointure GraphQL : l'émetteur de jointures GraphQL construit sa clause `ON` pour une seule paire de colonnes et n'a pas de place pour le second saut, si bien que les arêtes de jonction sont exclues du SDL généré et de `pg_constraint`. [tool-verified: `provisa/compiler/schema_gen.py:304`] La table de jonction reste interrogeable comme son propre champ racine, et disparaît du côté nœuds du schéma du graphe Cypher, de sorte qu'elle n'apparaît jamais comme label de nœud.

`materialize: true` fonctionne sur une arête de jonction, et ce qu'il matérialise est le parcours plutôt qu'une jointure directe `pets`-vers-`pets` : la vue contient le saut source, le saut de jonction, le discriminant et les colonnes propres de la jonction à côté de celles de la cible. Comme la jonction est une troisième patte de la jointure, le fait que l'arête franchisse des sources se juge sur les trois tables — une jonction dans une source différente des deux qu'elle relie est matérialisée même quand ces deux-là concordent. Une déclaration matérialise un type d'arête, donc une vue construite pour `bonded pair` ne répond jamais à un parcours `littermate`.

Valeurs de cardinalité [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81] :

- `many-to-one` — chaque ligne source correspond à une ligne cible (clé étrangère vers clé primaire)
- `one-to-many` — chaque ligne source correspond à plusieurs lignes cibles (inverse du précédent)

---

## Règles de sécurité au niveau des lignes {: #row-level-security-rules }

Les règles RLS injectent des clauses `WHERE` au moment de la requête, dans la portée d'un rôle et facultativement d'une table ou d'un domaine. [tool-verified: `provisa/core/models.py` `RLSRule` class lines 391–395 ; `config/provisa.yaml` lines 128–131] (REQ-041)

```yaml
rls_rules:

  - table_id: orders          # applies to orders table only
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"

  - domain_id: sales-analytics  # applies to every table in domain (REQ-402)
    role_id: analyst
    filter: "tenant_id = current_setting('provisa.tenant_id')"
```

Lorsqu'une règle au niveau du domaine et une règle au niveau de la table coexistent pour le même rôle, la règle au niveau de la table l'emporte (REQ-403).

| Champ | Requis | Description |
| ------- | ---------- | ------------- |
| `table_id` | Conditionnel | Table à laquelle appliquer la règle ; mutuellement exclusif avec `domain_id` |
| `domain_id` | Conditionnel | Domaine auquel appliquer la règle ; s'applique à toutes les tables du domaine (REQ-402) |
| `role_id` | Oui | Rôle auquel s'applique cette règle |
| `filter` | Oui | Prédicat SQL injecté dans le `WHERE` ; peut référencer des variables de session (REQ-041) |

---

## Fonctions et webhooks {: #functions-and-webhooks }

### Fonctions de base de données {: #db-functions }

Suivez une fonction de base de données et exposez-la comme requête ou mutation GraphQL. [tool-verified: `provisa/core/models.py` `Function` class lines 423–438 ; `config/provisa.yaml` lines 152–164] (REQ-205)

Les sources de type base de données peuvent aussi découvrir automatiquement leurs procédures stockées et leurs fonctions depuis le catalogue du fournisseur (`pg_proc`, `information_schema.routines`, ou les équivalents du fournisseur), évitant d'avoir à enregistrer chacune à la main. La découverte lit `prokind` et `provolatile` : les fonctions immuables ou stables s'enregistrent comme relations paramétrées (les arguments de la procédure deviennent des paramètres de requête, sur la même forme que les tables GET d'OpenAPI), et les procédures volatiles s'enregistrent comme mutations ou fonctions suivies. Les routines découvertes traversent la gouvernance d'étape 2 exactement comme celles enregistrées à la main. [tool-verified: `provisa/api/admin/introspect.py:541`, `provisa/api/admin/introspect.py:593`] (REQ-887)

```yaml
functions:

  - name: get_customers_by_region
    source_id: sales-pg
    schema: public
    function_name: get_customers_by_region
    returns: customers
    domain_id: sales-analytics
    description: "Returns customers filtered by region"
    visible_to: [admin, analyst]
    kind: query
    arguments:

      - name: p_region
        type: String
```

| Champ | Requis | Par défaut | Description |
| ------- | ---------- | --------- | ------------- |
| `name` | Oui | — | Nom du champ GraphQL |
| `source_id` | Oui | — | Source contenant la fonction |
| `schema` | Non | `public` | Schéma de la base |
| `function_name` | Oui | — | Nom réel de la fonction dans la base |
| `returns` | Oui | — | Identifiant de la table enregistrée que la fonction retourne (REQ-207) |
| `arguments` | Non | `[]` | Liste de définitions d'arguments `{name, type}` (REQ-211) |
| `visible_to` | Non | `[]` | Rôles auxquels la fonction est affectée (vide : tous les rôles). Une `mutation` exige en plus le droit `write` |
| `domain_id` | Non | `""` | Domaine auquel appartient cette fonction |
| `description` | Non | `null` | Description du champ GraphQL |
| `kind` | Non | `mutation` | `"query"` ou `"mutation"` (REQ-205) |

### Webhooks

Exposez un endpoint HTTP externe comme requête ou mutation GraphQL. [tool-verified: `provisa/core/models.py` `Webhook` class lines 441–455 ; `config/provisa.yaml` lines 166–178] (REQ-209)

```yaml
webhooks:

  - name: notify_support
    url: http://localhost:9999/notify
    method: POST
    timeout_ms: 3000
    domain_id: sales-analytics
    description: "Send a support notification"
    visible_to: [admin]
    kind: mutation
    arguments:

      - name: message
        type: String
```

| Champ | Requis | Par défaut | Description |
| ------- | ---------- | --------- | ------------- |
| `name` | Oui | — | Nom du champ GraphQL |
| `url` | Oui | — | URL de l'endpoint du webhook |
| `method` | Non | `POST` | Méthode HTTP |
| `timeout_ms` | Non | `5000` | Délai d'attente de la requête en millisecondes |
| `returns` | Non | `null` | Identifiant de table enregistrée, ou null pour un type en ligne |
| `inline_return_type` | Non | `[]` | Liste de champs `{name, type}` pour des formes de retour personnalisées (REQ-210) |
| `arguments` | Non | `[]` | Liste de définitions d'arguments `{name, type}` |
| `visible_to` | Non | `[]` | Rôles pouvant appeler ce webhook |
| `domain_id` | Non | `""` | Domaine auquel appartient ce webhook |
| `description` | Non | `null` | Description du champ GraphQL |
| `kind` | Non | `mutation` | `"query"` ou `"mutation"` |

---

## Authentification {: #authentication }

L'authentification se configure sous la clé `auth`. [tool-verified: `provisa/core/models.py` `AuthConfig` class lines 467–477] (REQ-120)

| Fournisseur | Description |
| ---------- | ------------- |
| `none` | Aucune authentification ; toutes les requêtes traitées comme le `default_role` |
| `firebase` | Firebase Authentication ; exige `project_id` et `service_account_key` (REQ-121) |
| `keycloak` | Keycloak OIDC (REQ-122) |
| `oauth` | OAuth 2.0 générique (REQ-123) |
| `simple` | Nom d'utilisateur et mot de passe sans fournisseur externe (REQ-124) |

```yaml
auth:
  provider: firebase
  assignments_source: provisa   # "claims" or "provisa"
  default_role: analyst
  default_assignments:

    - role_id: analyst
      domain_id: "*"
  firebase:
    project_id: ${env:FIREBASE_PROJECT_ID}
    service_account_key: ${env:FIREBASE_SERVICE_ACCOUNT_KEY}
```

`assignments_source: claims` lit les attributions de rôles depuis les revendications du JWT. `assignments_source: provisa` les lit depuis le magasin d'attributions propre à Provisa. [tool-verified: `provisa/core/models.py` line 476] (REQ-551)

---

## Routage d'exécution {: #execution-routing }

**Exécution directe** — les requêtes SGBDR mono-source sont routées vers le pilote natif pour une latence inférieure à 100 ms (REQ-027). Les sources exigent à la fois une entrée `SOURCE_TO_DIALECT` et une entrée `SOURCE_TO_CONNECTOR` pour emprunter ce chemin (REQ-229).

**Exécution fédérée** — les requêtes multi-sources et les sources dépourvues de pilote direct passent par le moteur de fédération (REQ-028). Provisa embarque un moteur de fédération ; pointez vers votre propre cluster compatible pour les déploiements à grande échelle (REQ-226).

**Statistiques** — à l'enregistrement, Provisa exécute `ANALYZE` contre chaque table publiée pour amorcer l'optimiseur fondé sur les coûts (nombre de lignes, fraction de valeurs nulles, valeurs distinctes, min/max). Les échecs sont journalisés et ne bloquent pas l'enregistrement (REQ-275).

---

## Sources graphe et sémantiques {: #graph-semantic-sources }

### Neo4j

Enregistrez une base de données graphe Neo4j comme source interrogeable. Les intendants rédigent des requêtes Cypher qui projettent des valeurs scalaires ; Provisa met les résultats en cache et les expose comme types GraphQL (REQ-295).

Les requêtes Cypher doivent employer des accesseurs de propriétés dans la clause `RETURN` (`RETURN n.id AS id, n.name AS name`) — retourner des objets nœuds est rejeté au moment de l'enregistrement (REQ-296).

#### Enregistrement par fichier de configuration (REQ-1668) {: #config-file-registration-req-1668 }

Déclarez une source `neo4j` et ses tables en YAML. Chaque table exige un `query_template` (le Cypher qui produit ses lignes) et des colonnes typées. La clé `query_template` est invalide pour tout autre type de source. [tool-verified: `provisa/core/config_loader.py:456-511`]

Une source exige `host`, `port` et `database`. [tool-verified: `provisa/core/config_loader.py:456-468`] Les lignes sont récupérées en envoyant `{"statements": [{"statement": <cypher>}]}` en POST à `/db/<database>/tx/commit` (l'API de transaction HTTP de Neo4j). Une réponse avec une liste `errors` non vide est traitée comme une requête échouée, pas comme un résultat vide. [tool-verified: `provisa/neo4j/source.py:71-82`, `provisa/api_source/caller.py:344-347`, `provisa/api_source/normalizers.py:49-74`]

Chaque colonne exige `data_type`. Le chargeur fait correspondre les types de configuration au type de colonne d'API utilisé au moment de la requête [tool-verified: `provisa/neo4j/persist.py:31-64`] :

| `data_type` de configuration | Type d'API |
|---|---|
| `varchar`, `text`, `string`, `char` | string |
| `integer`, `int`, `bigint`, `smallint` | integer |
| `float`, `double`, `real`, `decimal`, `numeric`, `number` | number |
| `boolean`, `bool` | boolean |
| `json`, `jsonb` | jsonb |

`varchar(N)` et `decimal(10,2)` sont acceptés — le type de base avant la parenthèse est utilisé.

L'enregistrement persiste une ligne `api_sources` et une ligne `api_endpoints` par table, de sorte que les tables survivent à un redémarrage sans relire le fichier. Les points d'entrée REST d'administration sous `/admin/sources/neo4j` écrivent les mêmes lignes. [tool-verified: `provisa/neo4j/persist.py:77-120`]

```yaml
sources:
  - id: graph
    type: neo4j
    host: neo4j
    port: 7474
    database: neo4j
    cache_ttl: 300

tables:
  - source_id: graph
    schema: neo4j
    table: person_skills
    query_template: >-
      MATCH (p:Person)-[:HAS_SKILL]->(s:Skill)
      RETURN p.name AS name, s.skill AS skill, p.experience AS years
    columns:
      - name: name
        data_type: varchar
      - name: skill
        data_type: varchar
      - name: years
        data_type: integer
```

#### Register Table dans l'interface (REQ-1670) {: #register-table-in-the-ui-req-1670 }

Une source neo4j n'a pas de tables à lister, donc le formulaire Register Table demande la table au lieu d'en proposer une. [tool-verified: `provisa-ui/src/pages/tables/RegisterTableForm.tsx` (`isNeo4j`)]

1. Choisissez la source neo4j et un domaine. Les sélecteurs de schéma et de table, la case à cocher de découverte et le sélecteur de filigrane n'apparaissent pas ; la source n'est jamais introspectée.
2. Saisissez un nom de table et le Cypher. Le Cypher doit projeter des scalaires (`RETURN a.name AS name`) ; une projection qui retourne un nœud ou une liste est signalée comme une erreur.
3. Appuyez sur Aperçu. Le formulaire exécute le Cypher avec `LIMIT 5` via la requête GraphQL `neo4jPreview` et remplit la liste des colonnes à partir des lignes retournées, typées comme `text`, `integer`, `double`, `boolean` ou `json`. [tool-verified: `provisa/api/admin/_neo4j_registration.py` `preview_neo4j`] Un aperçu échoué conserve le Cypher dans l'éditeur et affiche le message.
4. Ajustez la visibilité, les alias ou le masquage comme pour toute table, puis enregistrez. Le formulaire refuse de soumettre tant qu'un aperçu n'a pas typé les colonnes, et le serveur refuse une table neo4j qui ne porte aucun Cypher (`schema.neo4j_query_required`). [tool-verified: `provisa/api/admin/schema_mutation_ops.py` `persist_neo4j_registration`]

Le Cypher est stocké avec la table comme `queryTemplate`, s'affiche sur la vue de lecture de la table, et persiste exactement comme le fait un enregistrement par fichier de configuration : une ligne `api_sources` et une ligne `api_endpoints` que le prochain démarrage hydrate. Modifier la table repersiste un Cypher modifié.

#### Enregistrement REST d'administration {: #admin-rest-registration }

```bash
# Register via admin API (no YAML config required)
POST /admin/sources/neo4j
{
  "source_id": "graph",
  "host": "neo4j",
  "port": 7474,
  "database": "neo4j"
}

# Register a table (preview + validate before persisting)
POST /admin/sources/neo4j/graph/tables
{
  "table_name": "person_skills",
  "cypher": "MATCH (p:Person)-[:HAS_SKILL]->(s:Skill) RETURN p.name AS name, s.skill AS skill, p.experience AS years",
  "ttl": 300
}
```

Le point d'entrée d'aperçu (`POST /admin/sources/neo4j/{id}/preview`) retourne des lignes d'exemple et bloque l'enregistrement si le Cypher retourne des objets nœuds (REQ-296).

### SPARQL

Enregistrez n'importe quel triplestore conforme à SPARQL 1.1 (Apache Jena Fuseki, Virtuoso, Stardog, etc.) comme source interrogeable (REQ-297).

Les requêtes doivent être des requêtes `SELECT`. Les noms de variables de la clause `SELECT` deviennent automatiquement des noms de colonnes (REQ-297).

```bash
# Register via admin API
POST /admin/sources/sparql
{
  "source_id": "knowledge-graph",
  "endpoint_url": "http://fuseki:3030/ds/sparql",
  "default_graph_uri": "http://example.org/graph"
}

# Register a table (executes LIMIT 5 probe to validate and infer columns)
POST /admin/sources/sparql/knowledge-graph/tables
{
  "table_name": "product_categories",
  "sparql_query": "SELECT ?product ?label ?category WHERE { ?product a :Product ; rdfs:label ?label ; :hasCategory ?category . }",
  "ttl": 600
}
```

Les deux connecteurs utilisent le pipeline de cache des sources d'API — les résultats sont stockés dans PostgreSQL avec un TTL configurable, ce qui les rend disponibles pour des JOIN fédérés inter-sources (REQ-295, REQ-297, REQ-299).

---

#### Enregistrement par fichier de configuration et par l'interface (REQ-1683) {: #config-file-and-ui-registration-req-1683 }

Le `host` d'une source `sparql` est l'URL de son point de terminaison SPARQL (le formulaire Sources le stocke de la même manière). Chaque table sous elle porte `query_template`, un SELECT dont les variables sont les colonnes ; chaque liaison est `text`. [tool-verified: `provisa/core/config_loader.py` `_validate_neo4j_sources`, `_handle_sparql_table`]

```yaml
sources:
- id: sparql-demo
  type: sparql
  host: http://localhost:23030/provisa/query
tables:
- source_id: sparql-demo
  domain_id: shelter
  schema: sparql
  table: volunteer
  query_template: >-
    PREFIX s: <http://provisa.dev/shelter#>
    SELECT ?volunteer_id ?name WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name }
  columns:
  - { name: volunteer_id, data_type: text, visible_to: [org_admin] }
  - { name: name, data_type: text, visible_to: [org_admin] }
```

Register Table fonctionne de la même manière que pour Neo4j : choisissez la source, saisissez un nom de table et le SELECT, appuyez sur Aperçu (la requête `sparqlPreview` l'exécute avec `LIMIT 5` et remplit la liste des colonnes), puis enregistrez. L'enregistrement persiste une ligne `api_sources` et une ligne `api_endpoints` (POST encodé en formulaire vers le chemin du point de terminaison, normaliseur `sparql_bindings`), les mêmes lignes qu'écrit un enregistrement par fichier de configuration, et le moteur natif atterrit les lignes via la même chaîne de récupération que Neo4j. [tool-verified: `provisa/api/admin/_query_api_registration.py`, `provisa/sparql/persist.py`]

## Exemples de connexion {: #connection-examples }

### PostgreSQL

```yaml
- id: sales-pg
  type: postgresql
  host: postgres
  port: 5432
  database: provisa
  username: provisa
  password: ${env:PG_PASSWORD}
```

### Snowflake

```yaml
- id: analytics-sf
  type: snowflake
  host: org.snowflakecomputing.com
  port: 443
  database: ANALYTICS
  username: svc_provisa
  password: ${env:SNOWFLAKE_PASSWORD}
  federation_hints:
    account: myorg-myaccount
    warehouse: COMPUTE_WH
```

### Databricks

```yaml
- id: lakehouse-db
  type: databricks
  host: my-workspace.azuredatabricks.net
  password: ${env:DATABRICKS_TOKEN}
  federation_hints:
    http_path: /sql/1.0/warehouses/xxxx
```

### MongoDB

```yaml
- id: reviews-mongo
  type: mongodb
  host: mongodb
  port: 27017
  database: provisa
  username: ""
  password: ""
```

### Requête inter-sources {: #cross-source-query }

```graphql
{
  orders(where: {region: {eq: "us"}}) {
    id
    amount
    customers {       # PostgreSQL
      name
      email
    }
    productReviews {  # MongoDB (federated)
      rating
      comment
    }
  }
}
```

Les portions mono-source sont routées directement (REQ-027). Les JOIN inter-sources se fédèrent avec coercition de types automatique (REQ-028, REQ-552).
