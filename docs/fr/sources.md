# Types de sources

## Modèle d'exécution

Toute requête finit par s'exécuter à travers le moteur de fédération, qui assure la fédération sur l'ensemble des sources. Les sources se répartissent en trois catégories selon leur connectivité. [tool-verified: `provisa/core/models.py` lines 84–132] (REQ-550)

| Catégorie | Pilote direct | Connecteur fédéré | Exemples |
| --- | --- | --- | --- |
| **Capable en direct** | Oui | Oui | PostgreSQL, MySQL, MariaDB, SingleStore, SQL Server, Oracle, DuckDB |
| **Fédération seule** | Non | Oui | Redshift, Druid, Exasol, Hive, Iceberg, Delta Lake, Hive (adossé à S3) |
| **Lecture directe (réplica)** | Oui | Oui | Snowflake, Databricks, ClickHouse — le pilote lit les données et dépose un réplica ; les requêtes s'exécutent contre le réplica dans le moteur actif |
| **Matérialisation → fédération** | Non | Non | REST/OpenAPI, GraphQL distant, gRPC, Cypher Neo4j, SPARQL, WebSocket, RSS, CSV, SQLite, Parquet, Ingest (récepteur push), GovData, SharePoint, Splunk |

Les sources **capables en direct** exécutent les requêtes mono-source via leur pilote natif (moins de 100 ms), en contournant le moteur de fédération (REQ-027, REQ-229). Elles conservent la prise en charge complète des connecteurs et participent à la fédération lorsqu'elles sont jointes à d'autres sources (REQ-028).

Les sources en **fédération seule** sont toujours interrogées à travers la couche de fédération. Aucun pilote direct n'existe (REQ-229).

Les sources en **lecture directe (réplica)** disposent d'un DirectDriver qui lit depuis l'entrepôt nativement (nativement Arrow lorsque c'est possible), dépose un réplica dans le magasin de matérialisation du moteur actif, puis les requêtes s'exécutent contre ce réplica. Voir [Entrepôts de données comme sources nommées](#entrepots-de-donnees-comme-sources-nommees).

Les sources à **matérialisation** n'ont pas de connecteur fédéré. Provisa récupère leurs données (au démarrage ou au moment de la requête) et les met en cache en Parquet sur S3 ou dans PostgreSQL, les rendant atteignables par le moteur de fédération pour des requêtes inter-sources (REQ-309).

---

## Toutes les sources

Provisa enregistre **53** types de sources. Les tableaux ci-dessous les couvrent tous les 53 ; l'index correspond au décompte. [tool-verified: `provisa/core/models.py` `SourceType`]

| # | Groupe | Types de sources |
| --- | --- | --- |
| 1–13 | [SGBDR](#sgbdr) | `postgresql`, `mysql`, `mariadb`, `singlestore`, `sqlserver`, `oracle`, `duckdb`, `cockroachdb`, `yugabytedb`, `greenplum`, `tidb`, `firebird`, `airport` |
| 14–20 | [Entrepôts de données cloud](#entrepots-de-donnees-cloud) | `snowflake`, `bigquery`, `databricks`, `redshift`, `fabric`, `synapse`, `trino` |
| 21–25 | [Analytique / OLAP](#analytique-olap) | `clickhouse`, `druid`, `exasol`, `elasticsearch`, `pinot` |
| 26–30 | [Lac de données / formats de tables ouverts](#lac-de-donnees-formats-de-tables-ouverts) | `iceberg`, `delta_lake`, `hudi`, `hive`, `hive_s3` |
| 31–33 | [NoSQL](#nosql) | `mongodb`, `cassandra`, `redis` |
| 34–36 | [Flux (streaming)](#flux-streaming) | `kafka`, `websocket`, `rss` |
| 37 | [Récepteur push](#recepteur-push) | `ingest` |
| 38–39 | [Graphe et sémantique](#graphe-et-semantique) | `neo4j`, `sparql` |
| 40–43 | [Fondées sur des fichiers](#fondees-sur-des-fichiers) | `sqlite`, `csv`, `parquet`, `files` |
| 44–45 | [Observabilité et autres](#observabilite-et-autres) | `google_sheets`, `prometheus` |
| 46–47 | [SaaS d'entreprise](#connecteurs-saas-dentreprise) | `sharepoint`, `splunk` |
| 48–50 | [Sources API](#sources-api) | `openapi`, `graphql_remote`, `grpc_remote` |
| 51 | [GovData](#govdata) | `govdata` |
| 52–53 | [Vérificateurs de qualité des données](#verificateurs-de-qualite-des-donnees-req-1443) | `soda`, `great_expectations` |

Référence de chaque type de source pris en charge par Provisa. « Pilote direct » signifie que les requêtes mono-source s'exécutent nativement contre la source (moins de 100 ms) (REQ-027). Le « nom du connecteur » est le connecteur fédéré employé lorsque la source participe à des JOIN multi-sources (REQ-028). [tool-verified: `provisa/core/source_registry.py` `SOURCE_TO_DIALECT`; `provisa/federation/trino_connectors.py` `trino_connector_name`]

### SGBDR

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

Les bases compatibles au niveau du protocole réutilisent le pilote JDBC, le pilote asynchrone natif et le dialecte du protocole de base — CockroachDB, YugabyteDB et Greenplum empruntent le protocole PostgreSQL ; TiDB emprunte le protocole MySQL. Ils n'exigent que des entrées de registre, aucun nouveau code de connecteur. [tool-verified: `provisa/core/source_registry.py` `_PG_WIRE_TYPES`, `_MYSQL_WIRE_TYPES`] (REQ-950)

`firebird` (Firebird 3/4/5) et `airport` (serveur Arrow Flight) sont des types de sources enregistrés atteints sur place via des extensions communautaires DuckDB lorsque DuckDB est le moteur actif — aucun pilote direct, aucun connecteur fédéré. [tool-verified: `provisa/core/models.py` lines 44, 93] (REQ-899)

### Entrepôts de données cloud

[tool-verified: `executor/drivers/snowflake.py`, `executor/drivers/databricks.py`, `executor/drivers/registry.py`]

| Type de source | Pilote direct | Nom du connecteur | Dialecte | Mutations | Remarques |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `snowflake` | SnowflakeDriver | snowflake | snowflake | Fédérées | Lit via snowflake-connector-python ; dépose un réplica ; `account`/`warehouse`/`role` dans `federation_hints` (REQ-988) |
| `bigquery` | — | bigquery | bigquery | Fédérées | Aucun DirectDriver ; atteint via le moteur de fédération ou l'ATTACH du moteur BigQuery |
| `databricks` | DatabricksDriver | delta_lake | databricks | Fédérées | Lit via databricks-sql-connector (Cloud Fetch, Arrow) ; dépose un réplica ; `http_path` requis dans `federation_hints` (REQ-987) |
| `redshift` | — | redshift | redshift | Fédérées | — |
| `fabric` | MssqlWarehouseDriver | — | tsql | Fédérées | Microsoft Fabric Warehouse ; T-SQL sur TDS, authentification Azure AD ; dépose un réplica (REQ-995) |
| `synapse` | MssqlWarehouseDriver | — | tsql | Fédérées | Azure Synapse SQL ; T-SQL sur TDS, authentification Azure AD ; dépose un réplica (REQ-995) |
| `trino` | SQLAlchemyDriver | — | — | Fédérées | Lecture d'un coordinateur Trino/Presto distant via le dialecte SQLAlchemy trino ; dépose un réplica sur n'importe quel moteur (REQ-994) |

### Analytique / OLAP

[tool-verified: `executor/drivers/clickhouse.py`]

| Type de source | Pilote direct | Nom du connecteur | Dialecte | Mutations | Remarques |
| ------------ | -------------- | ----------------- | ----------------- | ----------- | ------- |
| `clickhouse` | ClickHouseDriver | clickhouse | clickhouse | Fédérées | Lit via clickhouse-connect (HTTP) ; `secure: "true"` dans `federation_hints` pour le TLS (REQ-986) |
| `druid` | — | druid | druid | Non | — |
| `exasol` | — | exasol | exasol | Non | — |
| `elasticsearch` | — | elasticsearch | — | Non | Les propriétés du connecteur proviennent du DSL de mapping du type [tool-verified: `trino_connectors.py:309`] |
| `pinot` | — | pinot | — | Non | Connecteur Trino `pinot` ; `pinot.controller-urls` = host:port du contrôleur Pinot [tool-verified: `trino_connectors.py:199`] |

### Lac de données / formats de tables ouverts

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
| `cassandra` | cassandra | Non |
| `redis` | redis | Non |

### Flux (streaming)

| Type de source | Mécanisme | Mutations |
| ------------ | ----------- | ----------- |
| `kafka` | Connecteur Kafka fédéré ; schéma via Confluent Schema Registry (Avro, Protobuf, JSON Schema), définition manuelle ou inférence sur échantillon (REQ-147, REQ-150) | Receveur uniquement (REQ-176) |
| `websocket` | Flux WebSocket externe — connexion, abonnement, réception d'événements ; résultats matérialisés (REQ-338) | Non |
| `rss` | Flux RSS 2.0 / Atom — interrogation, filigrane par pubDate/updated ; résultats matérialisés (REQ-342, REQ-343) | Non |

### Récepteur push

| Type de source | Mécanisme | Mutations |
| ------------ | ----------- | ----------- |
| `ingest` | Des services externes POSTent des événements JSON ; résultats matérialisés (REQ-331, REQ-335) | Non |

### Graphe et sémantique

| Type de source | Mécanisme | Mutations |
| ------------ | ----------- | ----------- |
| `neo4j` | Cypher via API HTTP, résultats mis en cache dans PostgreSQL (REQ-295) | Non |
| `sparql` | SPARQL 1.1 en POST, résultats mis en cache dans PostgreSQL (REQ-297) | Non |

### Fondées sur des fichiers

Deux mécanismes couvrent les fichiers. Tous deux emploient le champ `path` au lieu de `host`/`port`. [tool-verified: `provisa/core/models.py`] (REQ-553)

**Sources à fichier unique** — `sqlite`, `csv`, `parquet` font pointer `path` vers un seul fichier.

| Type de source | Transports | Mutations |
| --- | --- | --- |
| `sqlite` | local | Oui |
| `csv` | local | Non |
| `parquet` | local, `s3://` | Non |

Les buckets privés exigent des identifiants (région et clés AWS depuis l'environnement). Pour du CSV sur `s3://` ou `http(s)://`, ou pour enregistrer plusieurs fichiers d'un coup, utilisez la source `files`. [tool-verified: `provisa/file_source/source.py`]

**Source `files`** — fait pointer `path` vers un motif glob, le parcourt récursivement et enregistre le répertoire comme catalogue fédéré de tables. Elle lit de nombreux formats sur de nombreux transports ; les ensembles ci-dessous proviennent du connecteur de fichiers (fork kenstott/calcite). [tool-verified: `provisa/core/catalog.py` `files` branch and `provisa/core/models.py` `SOURCE_TO_CONNECTOR`; format and transport lists from the calcite `file` adapter — `FileSchema.java`, `storage/StorageProviderFactory.java`]

| Formats | Transports |
| --- | --- |
| CSV, TSV, JSON, YAML, Excel (XLS/XLSX), Parquet, Arrow, et documents convertis en tables — HTML, Markdown, DOCX, PPTX | Système de fichiers local, HTTP(S), `s3://`, `hdfs://`, `ftp://`/`ftps://`, `sftp://`, `iceberg://`, SharePoint (REST et Microsoft Graph) |

```yaml
- id: sales_files
  type: files
  path: s3://bucket/sales/**/*.csv   # glob; local and http(s):// also supported
```

### Observabilité et autres

`prometheus` dispose d'un connecteur Trino (propriétés construites à partir du DSL de mapping du type). `google_sheets` est un type de source enregistré sans connecteur Trino, qui se matérialise via le pipeline de cache d'API. [tool-verified: `provisa/federation/trino_connectors.py:314`; `provisa/core/models.py` lines 87–88]

| Type de source | Nom du connecteur | Mutations |
| ------------ | ----------------- | ----------- |
| `google_sheets` | — (matérialisé) | Non |
| `prometheus` | prometheus | Non |

### Connecteurs SaaS d'entreprise

SharePoint et Splunk s'enregistrent via des connecteurs Apache Calcite (fork kenstott/calcite). Ni l'un ni l'autre n'a de pilote direct — Provisa matérialise leurs lignes en lançant le serveur pgwire Calcite fourni avec le connecteur (`pgwire-sharepoint`, `pgwire-splunk`), en s'y connectant comme à un endpoint PostgreSQL générique et en déposant les lignes dans le magasin de matérialisation pour la fédération (REQ-954). Les deux connecteurs activent toujours la correspondance de noms insensible à la casse, conformément aux sémantiques insensibles à la casse de chaque produit (REQ-725, REQ-730). [tool-verified: `provisa/core/models.py` lines 99–100 ; `provisa/federation/trino_connectors.py` lines 223–286]

#### `sharepoint`

Les listes SharePoint sont énumérées comme des schémas et exposées comme des tables interrogeables (REQ-726, REQ-731). Deux méthodes d'authentification : `CLIENT_CREDENTIALS` (par défaut) et par certificat via un certificat PFX (REQ-727). Les valeurs secrètes de `mapping` sont résolues par le moteur de secrets avant d'atteindre le connecteur (REQ-729). [tool-verified: `provisa/federation/trino_connectors.py` lines 230–252]

| Champ de la source | Propriété du connecteur | Remarques |
| --- | --- | --- |
| `base_url` ou `host` | `site-url` | URL du site SharePoint |
| `username` | `client-id` | Identifiant client de l'application Azure |
| `password` | `client-secret` | Secret client de l'application Azure |
| `database` | `tenant-id` | UUID du locataire Azure |
| `mapping.auth_type` | `auth-type` | `CLIENT_CREDENTIALS` (par défaut) ou `CERTIFICATE` |
| `mapping.certificate_path` | `certificate-path` | Chemin du PFX quand `auth_type: CERTIFICATE` |
| `mapping.certificate_password` | `certificate-password` | Mot de passe du PFX |

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

### Sources API

Enregistrez n'importe quel endpoint HTTP comme table interrogeable. [tool-verified: `provisa/core/models.py` `SourceType` enum] (REQ-314, REQ-307, REQ-322)

| Type d'API | Découverte | Inférence des colonnes |
| --------- | ----------- | ----------------- |
| `openapi` | Analyse de la spécification OpenAPI (REQ-314, REQ-316) | Primitifs → natifs, objets → JSONB |
| `graphql_remote` | Introspection du schéma (REQ-307, REQ-308) | Primitifs → natifs, objets → JSONB |
| `grpc_remote` | Réflexion serveur (REQ-322, REQ-325) | Primitifs → natifs, objets → JSONB |

Les réponses d'API sont récupérées, mises en cache dans PostgreSQL (TTL configurable) et exposées comme types GraphQL (REQ-309, REQ-318, REQ-327). Les tables mises en cache participent aux requêtes fédérées comme n'importe quelle autre source (REQ-313).

**Règles JSONB** : les colonnes complexes (objets, tableaux) stockées en JSONB ne sont pas filtrables (REQ-119). L'accès aux sous-champs passe par l'extraction `->>` en SQL (REQ-151). Les relations sont déclarées entre tables à l'aide de colonnes de clé étrangère scalaires — les colonnes JSONB brutes ne sont pas des cibles de jointure. Utilisez la promotion JSONB pour convertir des champs imbriqués en colonnes scalaires natives lorsqu'il faut filtrer ou joindre dessus (REQ-119).

### GovData

Données ouvertes du gouvernement américain. L'accès est partitionné par regroupement thématique. [tool-verified: `provisa/core/models.py` lines 543–609]

Chaque source `govdata` sélectionne un thème. Ce thème détermine quels schémas GovData sont exposés. Les schémas `ref` et `geo` sont toujours inclus comme schémas de liaison — ils ne sont pas listés par thème mais sont toujours présents. [tool-verified: `provisa/core/models.py` line 562–563 comment]

| Thème | Schémas exposés |
| --------- | ----------------- |
| `COMMERCE` | `sec`, `patents` |
| `ECONOMY` | `econ` |
| `EDUCATION` | `census`, `edu` |
| `HEALTH` | `health` |
| `CYBER` | `cyber_threat`, `cyber_vuln` |
| `PUBLIC_SAFETY` | `crime` |
| `ENVIRONMENT` | `lands` |
| `WEATHER` | `weather` |
| `GOVERNMENT` | `fedregister`, `fec` |
| `ALL` | Tous les schémas ci-dessus |

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

### Vérificateurs de qualité des données (REQ-1443)

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

## Connecteurs personnalisés (REQ-1177)

Les moteurs de fédération natifs — Postgres, DuckDB et ClickHouse — gagnent l'accès à un nouveau type de source lorsqu'un exploitant déclare un connecteur pour ce type dans `config/custom_connectors.yaml`. Aucun code n'est requis. [tool-verified: `provisa/federation/custom_connectors.py` `load_custom_connectors`; `provisa/federation/engine.py` `build_pg_engine`, `build_duckdb_engine`, `build_clickhouse_engine`]

L'extensibilité des connecteurs elle-même est antérieure. Le moteur Trino est extensible de longue date à son propre niveau — un connecteur JDBC générique paramétré par type de source, un corps de catalogue `.properties` par type, et les greffons de connecteurs Trino personnalisés de Provisa (Splunk, SharePoint, Calcite). [tool-verified: `provisa/federation/trino_connectors.py` `_TrinoJdbcConnector`, `_TRINO_JDBC_TYPES`; `trino/plugins/trino-splunk`, `trino/plugins/trino-sharepoint`, `trino/plugins/trino-calcite`] REQ-1177 apporte cette même extensibilité pilotée par configuration aux deux moteurs natifs sans cluster, qui portaient auparavant un jeu de connecteurs figé.

La configuration est livrée vide. Les connecteurs intégrés couvrent la portée prête à l'emploi ; tout ce que contient ce fichier est rédigé par l'exploitant. [tool-verified: `config/custom_connectors.yaml` line 52: `connectors: []`] Définissez `PROVISA_CUSTOM_CONNECTORS` pour pointer vers un autre chemin (utile pour les tests).

### Genres de descripteurs

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

### Contrôle par sondage

La disponibilité est vérifiée au moment de l'attachement contre le catalogue de découverte standard de chaque moteur :

- **Postgres** — vérifie `pg_extension`, puis `pg_available_extensions`. [tool-verified: `provisa/federation/connector_duckdb.py` `_probe_pg_extension` lines 333–344]
- **DuckDB** — exécute `INSTALL`/`LOAD` et vérifie `duckdb_functions()` pour le `probe_symbol` déclaré. [tool-verified: `provisa/federation/connector_duckdb.py` `_DuckDBExtensionConnector.probe` lines 160–180]
- **ClickHouse** — vérifie `system.table_engines` pour le `ch_engine` déclaré ; son absence de la build échoue bruyamment. [tool-verified: `provisa/federation/custom_connectors.py` `_probe_clickhouse_engine`]

Une extension déclarée mais non installable échoue bruyamment. Aucun saut silencieux, aucun repli. Un connecteur dont le sondage échoue n'est simplement pas actif pour ce déploiement.

### Variables de gabarit

Chaque valeur de `server_options`, chaque valeur de `user_mapping`, chaque `attach_template` et chaque `scan_template` peut employer des caractères de remplacement `{field}`. Champs disponibles : [tool-verified: `provisa/federation/custom_connectors.py` `_source_fields` lines 53–63]

`{id}`, `{host}`, `{port}`, `{database}`, `{username}`, `{password}`, `{path}`, `{schema_name}`, `{table_name}`, plus toute clé issue de `federation_hints`. Les gabarits d'attachement DuckDB reçoivent également `{alias}` — l'alias de catalogue interne que Provisa attribue à la base attachée.

Un gabarit référençant un champ inconnu échoue bruyamment au moment de l'attachement, faisant apparaître une discordance descripteur/source avant qu'un DDL cassé n'atteigne le moteur.

### Exemples

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

## Entrepôts de données comme sources nommées

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

## Champs de configuration des sources

Toutes les sources partagent un jeu commun de champs. [tool-verified: `provisa/core/models.py` `Source` class, lines 138–204]

| Champ | Requis | Par défaut | Description |
| ------- | ---------- | --------- | ------------- |
| `id` | Oui | — | Identifiant unique ; alphanumérique avec tirets/soulignés |
| `type` | Oui | — | Type de source (voir les tableaux ci-dessus) |
| `host` | Non | `""` | Nom d'hôte ou IP |
| `port` | Non | `0` | Numéro de port |
| `database` | Non | `""` | Nom de la base |
| `username` | Non | `""` | Nom d'utilisateur |
| `password` | Non | `""` | Mot de passe ; utilisez `${env:VAR}` pour la résolution de secrets |
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
| `mapping` | Non | `{}` | Réglages de connecteur propres au type pour les sources NoSQL et SaaS (par exemple `auth_type` SharePoint, `use_token` Splunk) (REQ-251) |
| `allowed_domains` | Non | `[]` | Restreindre la source à des domaines précis ; vide = sans restriction |
| `description` | Non | `""` | Description lisible |

---

## Sources Kafka

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

## Visibilité des colonnes

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

## Relations

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
| `materialize` | Non | Créer automatiquement une vue matérialisée pour les jointures inter-sources (REQ-158) |
| `refresh_interval` | Non | Intervalle de rafraîchissement de la MV en secondes (par défaut : 300) |
| `target_function_name` | Non | Nom de la fonction de base de données pour les relations calculées |
| `function_arg` | Non | Quel argument de la fonction reçoit la valeur de la colonne source |
| `alias` | Non | Type de relation lisible (par exemple `WORKS_FOR`) |
| `graphql_alias` | Non | Nomme le champ SDL que cette relation expose sur le type parent. En son absence, le nom est dérivé du `field_name` de la table cible et de la cardinalité de la relation. [tool-verified: `provisa/compiler/schema_gen.py:1050`] |
| `disable_cypher` | Non | Quand `true`, exclure cette relation des arêtes du graphe Cypher |
| `source_json_key` | Non | Extraire cette clé de la colonne source comme objet JSON avant le JOIN |

Valeurs de cardinalité [tool-verified: `provisa/core/models.py` `Cardinality` enum, lines 79–81] :

- `many-to-one` — chaque ligne source correspond à une ligne cible (clé étrangère vers clé primaire)
- `one-to-many` — chaque ligne source correspond à plusieurs lignes cibles (inverse du précédent)

---

## Règles de sécurité au niveau des lignes

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

## Fonctions et webhooks

### Fonctions de base de données

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
| `visible_to` | Non | `[]` | Rôles pouvant appeler cette fonction |
| `writable_by` | Non | `[]` | Rôles pouvant l'appeler comme mutation |
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

## Authentification

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

## Routage d'exécution

**Exécution directe** — les requêtes SGBDR mono-source sont routées vers le pilote natif pour une latence inférieure à 100 ms (REQ-027). Les sources exigent à la fois une entrée `SOURCE_TO_DIALECT` et une entrée `SOURCE_TO_CONNECTOR` pour emprunter ce chemin (REQ-229).

**Exécution fédérée** — les requêtes multi-sources et les sources dépourvues de pilote direct passent par le moteur de fédération (REQ-028). Provisa embarque un moteur de fédération ; pointez vers votre propre cluster compatible pour les déploiements à grande échelle (REQ-226).

**Statistiques** — à l'enregistrement, Provisa exécute `ANALYZE` contre chaque table publiée pour amorcer l'optimiseur fondé sur les coûts (nombre de lignes, fraction de valeurs nulles, valeurs distinctes, min/max). Les échecs sont journalisés et ne bloquent pas l'enregistrement (REQ-275).

---

## Sources graphe et sémantiques

### Neo4j

Enregistrez une base de données graphe Neo4j comme source interrogeable. Les intendants rédigent des requêtes Cypher qui projettent des valeurs scalaires ; Provisa met les résultats en cache et les expose comme types GraphQL (REQ-295).

Les requêtes Cypher doivent employer des accesseurs de propriétés dans la clause `RETURN` (`RETURN n.id AS id, n.name AS name`) — retourner des objets nœuds est rejeté au moment de l'enregistrement (REQ-296).

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

L'endpoint d'aperçu (`POST /admin/sources/neo4j/{id}/preview`) retourne des lignes d'exemple et bloque l'enregistrement si le Cypher retourne des objets nœuds (REQ-296).

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

## Exemples de connexion

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

### Requête inter-sources

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
