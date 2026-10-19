# Répliques {: #replicas }

Une réplique est une copie d'une table source que Provisa conserve dans le magasin de matérialisation du moteur et lit à la place de la source. Qu'une table soit répliquée ou non est déterminé par [`replicate`](configuration.md#the-replicate-setting) et par ce que le moteur peut atteindre ; voir [Ce qu'un moteur change](engines.md#what-an-engine-changes).

## Les répliques sont indexées {: #replicas-are-indexed }

Provisa indexe une réplique selon la façon dont le modèle utilise sa table.

- **La clé primaire déclarée**, appliquée comme clé.
- **Les colonnes sur lesquelles les relations déclarées font la jointure** :
  - la clé étrangère de chaque relation dont la source est cette table ;
  - la colonne cible de chaque relation dont la cible est cette table, sauf si cette colonne est déjà la clé ;
  - les deux clés étrangères d'une table de jonction qu'une relation traverse.

Prenons cette relation :

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

La réplique de `orders` reçoit sa clé et un index sur `customer_id`. La réplique de `customers` reçoit sa clé et, lorsque `id` est cette clé, n'a besoin de rien de plus. Une jointure des deux lit alors un index de chaque côté, dans un magasin qui utilise les index pour les jointures (voir [Ce que construit chaque magasin](#what-each-store-builds)). Une relation portant sur une colonne calculée que la réplique ne contient pas ne reçoit aucun index.

Déclarez la clé avec `is_primary_key: true` sur la colonne. Une réplique sans clé déclarée ne reçoit que ses index de jointure.

### Lisible d'abord, indexée ensuite {: #readable-first-indexed-after }

Un atterrissage n'attend jamais les index. La réplique est lue dès l'instant où elle est substituée ; le runner de répliques construit ensuite ses index un par un, dans une passe distincte, sous un verrou d'index par réplique. Attendez-vous à ce que les premières requêtes après un atterrissage s'exécutent sans eux.

Chaque atterrissage consigne les index qu'il a demandés, ceux qui ont été construits et ceux qui ont échoué, par nom, dans l'enregistrement de la réplique dans le plan de contrôle (`replica_state.index_state`).

- **Un index en échec ne fait jamais échouer l'atterrissage.** Il est journalisé avec sa cause et redemandé par l'atterrissage suivant. La ligne du journal est `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`.
- **Des lignes qui répètent une clé déclarée constituent un échec à part**, code `replica.index_duplicate_key`, qui nomme les colonnes de la clé. La source contient des doublons d'un ensemble de colonnes que le modèle déclare unique. Une nouvelle tentative n'y change rien : corrigez les données à la source, ou corrigez la clé déclarée.
- **Un atterrissage qui remplace la table en cours de construction** laisse derrière lui les constructions antérieures ; elles s'arrêtent et ne consignent rien, et la nouvelle table reçoit les siennes. Sur un magasin PostgreSQL, la substitution annule une construction encore en cours sur la table qu'elle remplace.
- **Le plan suit le modèle à chaque atterrissage.** Une relation ajoutée plus tard reçoit son index au prochain atterrissage de la réplique.

L'interface d'administration n'affiche pas l'enregistrement des index. Lisez le journal du serveur, ou la colonne `index_state` de la ligne de la réplique.

### Ce que construit chaque magasin {: #what-each-store-builds }

| Magasin | Ce qui est construit | Comment |
| --- | --- | --- |
| PostgreSQL : la base propre du moteur `pg`, le magasin PostgreSQL de Trino ou de DuckDB, le moteur `sqlalchemy` sur PostgreSQL | Index de clé et de jointure | `CREATE INDEX CONCURRENTLY` sur sa propre connexion, de sorte que lecteurs et écrivains ne sont pas bloqués. La clé est construite comme index unique, puis devient la clé primaire par `ADD CONSTRAINT ... PRIMARY KEY USING INDEX`. Un index invalide laissé par une construction annulée est supprimé puis reconstruit. |
| Autres bases derrière un moteur relationnel (`mysql`, `oracle`, `mssql` et les autres) | Index de clé et de jointure | Le DDL propre au dialecte. Une base qui refuse le DDL consigne l'index comme échoué. |
| Fichier DuckDB embarqué, MotherDuck | Index de clé et de jointure | Par le broker du magasin : `ALTER TABLE ... ADD PRIMARY KEY`, `CREATE INDEX IF NOT EXISTS`. |
| ClickHouse | La clé de tri uniquement | Une réplique ClickHouse est ordonnée par sa clé déclarée (`ORDER BY`), fixée à l'atterrissage. ClickHouse n'a pas d'index B-tree secondaire à construire. |
| Snowflake, BigQuery, Databricks, Fabric, Synapse | Rien | L'entrepôt de données organise lui-même la table. |

Un index n'aide qu'une jointure qui s'exécute là où il se trouve. Deux moteurs lisent leur magasin sans se servir de ses index pour les jointures :

- **DuckDB avec son fichier embarqué.** Le moteur n'attache jamais le fichier. Chaque instruction lit sa propre copie locale de chaque table du magasin qu'elle nomme, refaite chaque fois que le magasin a changé. Les jointures s'exécutent comme des jointures par hachage sur cette copie ; les index de jointure ne les accélèrent donc pas ; les index du fichier appliquent la clé et servent tout ce qui lit le fichier sur place.
- **DuckDB avec un magasin PostgreSQL.** DuckDB lit le magasin via son extension `postgres` avec le pushdown de filtres désactivé (bogue amont duckdb/duckdb-postgres#564) ; il parcourt donc la réplique, puis filtre et joint dans DuckDB. Les index du magasin sont construits, mais ces lectures ne s'en servent pas.

Un magasin MotherDuck est attaché et lu sur place.

## Utiliser une base relationnelle comme magasin de répliques là où le moteur y exécute le SQL {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

Quand le moteur exécute ses jointures à l'intérieur d'une base relationnelle, conservez-y les répliques. Les jointures lisent alors les index ci-dessus. Trois configurations le font :

- **Le moteur `pg`.** Les répliques atterrissent dans la base PostgreSQL propre du moteur et chaque instruction s'y exécute, via leurs index.
- **Un moteur relationnel** (`mysql`, `oracle`, `mssql`, le fourre-tout `sqlalchemy` et les autres). Chaque source atterrit dans cette base et y est interrogée.
- **Trino avec son magasin PostgreSQL.** PostgreSQL est le seul magasin que Trino prend en charge ; Trino lit les répliques par son connecteur PostgreSQL. Un filtre ou une jointure que Trino délègue à PostgreSQL utilise les index ; le travail que Trino fait lui-même, non.

Dirigez un moteur vers un magasin avec `PROVISA_MATERIALIZE_URL` ou `materialize_store_url` ; le magasin enregistré par l'organisation elle-même est prioritaire sur les deux.

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

Il s'agit d'une recommandation générale, pas d'une règle. Son intérêt dépend du moteur, des sources, du magasin et de la taille des tables. Pesez les points suivants :

- **Portée par requête.** Un magasin que le moteur atteint par le réseau ajoute un aller-retour et un transfert à chaque lecture. Le fichier DuckDB embarqué est local et en colonnes, et se parcourt vite.
- **Coût de l'atterrissage.** Écrire une réplique dans une base relationnelle, puis construire ses index, coûte plus cher qu'écrire un fichier local.
- **La charge propre de la base.** Les lectures de répliques, les atterrissages et les constructions d'index s'exécutent tous sur cette base. Dimensionnez-la en conséquence.
- **Grands parcours.** Un index n'apporte rien à une requête qui lit l'essentiel d'une grande table. Un magasin en colonnes la parcourt plus vite qu'un magasin en lignes.

## Choisir où vivent les répliques {: #choosing-where-replicas-live }

Le magasin est un levier parmi plusieurs, et ils se recoupent. Réglez d'abord les autres ; le magasin ne décide que de ce qu'ils laissent.

| Levier | Ce qu'il fait | Où il aide |
| --- | --- | --- |
| Tables hot | Une petite table de référence est conservée dans Redis et insérée dans chaque requête qui la lit sous forme de CTE (expression de table commune) `VALUES` en ligne. | Jointures vers de petites tables de référence, sur n'importe quel magasin : les lignes voyagent dans la requête. La cible d'une relation plusieurs-à-un, ou une table d'au plus `hot_tables.auto_threshold` lignes (1 000 par défaut), devient hot à sa première lecture. `hot: true` force l'inclusion d'une table et `hot: false` la maintient dehors. |
| Vues matérialisées que vous définissez | Une vue avec `materialize: true` est stockée et rafraîchie. Une requête qui lit la vue est servie depuis les lignes stockées quand elle est fraîche et que le lecteur la voit en entier : aucune entrée ne porte une règle de lignes plus étroite que tout. | Agrégats sur de grandes tables. Vous décidez lesquelles existent ; aucune n'est créée pour vous. Voir [Vues](configuration.md#views-governed-computed-datasets). |
| Relation avec `materialize: true` | Une relation entre sources reçoit une vue parent-enfant stockée et pré-jointe. Une requête dont la jointure correspond au motif, en totalité ou en partie, est réécrite pour la lire, qu'elle arrive en SQL ou soit construite par le compilateur. | Jointures sur un magasin qui ne construit pas d'index de jointure. Voir [Matérialisation automatique](configuration.md#auto-materialization). |
| Placement | Les sources que le moteur peut lire en direct sont lues en direct ; les autres sont répliquées. Ce qui finit dans le magasin découle du moteur que vous choisissez. | Le choix du moteur décide de l'essentiel de ce que le magasin doit contenir. |

La matérialisation de relations et les jointures indexées répondent au même besoin de deux façons. Sur un magasin en colonnes ou d'entrepôt (le fichier DuckDB, ClickHouse, Snowflake, BigQuery, Databricks, Fabric, Synapse), aucun index de jointure n'est construit ; une vue pré-jointe est donc le moyen d'alléger une jointure fréquente. Sur un magasin relationnel, les colonnes de jointure sont déjà indexées, et les mêmes jointures peuvent s'exécuter vite sans être matérialisées.

Ce qui dépend encore du magasin : les parcours et jointures ad hoc sur de grandes tables qu'aucune vue matérialisée ne couvre et qu'aucune table hot ne sert. Ceux-ci s'appuient sur les index du magasin dans une base relationnelle, ou sur sa vitesse de parcours dans une base en colonnes.

Les résultats varient selon la charge. Mesurez avant et après un changement avec l'[outil de dimensionnement](sizing.md) ; ses [exécutions en direct ou en réplique](sizing.md#live-or-replica-reads-and-the-ttl) donnent le coût d'une lecture en réplique par rapport à celui d'une lecture en direct sur votre déploiement.

## Fraîcheur {: #freshness }

Faites de la fraîcheur une part de la décision, à côté de la vitesse. L'ancienneté acceptable d'une réponse, et ce que coûte à la source le fait de la maintenir à jour, déterminent si une table est lue en direct ou depuis une réplique, et donc ce que le magasin doit contenir.

Les sources d'entreprise prennent rarement en charge le temps réel. Les données arrivent par chargement par lots ou ETL nocturne, les flux de changements prennent du retard, et beaucoup de systèmes ne supportent pas des lectures en direct au volume des requêtes. Partez de la fraîcheur dont chaque table a besoin, puis choisissez la stratégie la moins coûteuse qui la satisfait. Les règles côté opérateur figurent dans [Fraîcheur et charge : qui décide](configuration.md#freshness-and-load-who-decides).

| Stratégie | Se règle avec | Ce qu'elle garantit | Ce qu'elle coûte à la source |
| --- | --- | --- | --- |
| Lecture en direct | `replicate: -1` | À jour au moment de la requête | Chaque requête lit la source |
| TTL | `change_signal: ttl`, `cache_ttl` | Une réplique pas plus ancienne que `cache_ttl` | Une reconstruction par `cache_ttl` ; avec une `watermark_column`, seulement les lignes au-delà du filigrane |
| Sonde de changement | `change_signal: probe` ou `ttl_probe`, `cache_ttl` | Reconstruite seulement après que la sonde signale un changement ; la sonde s'exécute tous les `cache_ttl` | Une sonde par intervalle, et une reconstruction seulement en cas de changement |
| Flux de changements push | Une source Kafka ou WebSocket ; `change_signal: debezium` sur une source dotée d'un bloc `cdc` ; `change_signal: native` sur PostgreSQL, MongoDB ou Cosmos DB | À jour dans la limite du retard du flux et du debounce | Kafka, WebSocket et Debezium : aucun après la première construction, le flux transporte les lignes. PostgreSQL, MongoDB et Cosmos DB : une reconstruction par rafale de changements |
| Signal externe | `change_signal: signal` | Reconstruit chaque fois que `POST /data/sources/{source_id}/signal` indique que la source a changé ; jamais sur une horloge | Une reconstruction par signal |
| TTL par rôle | `role_ttl` sur une table | Un rôle listé accepte une réplique jusqu'à son propre TTL ; le `cache_ttl` de la table reste le plancher | Moins de reconstructions : la lecture d'un rôle tolérant n'en demande jamais |
| Protection de charge | `load_protected: true`, avec `cache_ttl`, une sonde ou une `off_peak_window` | Les requêtes ne lisent jamais la source ; seul le runner rafraîchit la réplique, et avec une fenêtre creuse seulement tant qu'elle est ouverte | Seulement les rafraîchissements planifiés |
| Porte de fraîcheur | `freshness_gate: true` sur une source | Chaque lecture est d'abord vérifiée par rapport au `change_signal` et au `cache_ttl` de la source ; un verdict périmé rafraîchit avant la lecture | Un rafraîchissement dans le chemin de lecture quand elle est périmée |
| N'expire jamais | `cache_ttl: -1` sur une source ou une table | Chargée une fois ; chaque lecture est servie par la réplique, et la lecture d'une table pas encore chargée est refusée en la nommant | Seulement ce chargement : une source dont toutes les tables sont chargées n'est plus jamais attachée, sondée ni lue, et n'a besoin d'aucun identifiant |

Une sonde qui n'a rien à comparer (ni filigrane, ni sentinelle, ni requête de sonde) se rabat sur le TTL.

### Quasi temps réel depuis un flux de changements {: #near-real-time-from-a-change-feed }

Lorsqu'une source ne peut pas supporter de charge de requêtes en direct mais publie déjà ses changements, un flux de changements donne des données quasi temps réel sans toucher la source au moment de la requête. Les requêtes lisent la réplique locale, rapide et sans charge pour la source, tandis que la boucle d'événements applique les changements à mesure qu'ils arrivent. Les données ne sont en retard que du retard du flux, typiquement quelques secondes.

Que le flux existe ne suffit pas. Provisa ne le suit que pour une table qui y est raccordée :

- **Une source Kafka.** La table nomme son topic, et chaque changement est appliqué à la réplique par clé primaire, suppressions comprises.

  ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **Une source WebSocket.** Son flux est appliqué de la même façon.
- **Une table avec `change_signal: debezium`** (sur la table ou sa source) sur une source dont le bloc `cdc` indique le transport Kafka et le préfixe de topic du connecteur. La première lecture construit le réplica ; ensuite, chaque événement de changement du topic `{topic_prefix}.{schema}.{table}` est appliqué par clé primaire, suppressions comprises, et l'offset n'est validé qu'une fois le changement chargé.
- **Une table d'une telle source avec `change_signal: kafka`**. Ses changements sont lus depuis le topic de deltas que nomme son `live.kafka.topic`, sur le même transport, et appliqués de la même façon ; une table sur ce signal qui ne nomme aucun topic est refusée nommément à l'enregistrement de la table ou de sa source et au chargement de la configuration.
- **Une table MongoDB ou Cosmos DB avec `change_signal: native`** (sur la table ou sur sa source). Chaque rafale de changements sur le flux de changements demande une reconstruction de la réplique.
- **Une table PostgreSQL avec `change_signal: native`** (sur la table ou sa source). Provisa lit la réplication logique de la base depuis un slot qui lui est propre, `provisa_<source id>`, sur une publication du même nom qui liste les tables natives de la source. Kafka n'intervient pas. Chaque rafale de changements sur une table demande une reconstruction de son réplica. La base a besoin de ce dont Debezium a besoin : `wal_level` à `logical`, un rôle doté du droit de réplication qui peut créer la publication, et un primaire plutôt qu'un réplica en lecture ; une source à laquelle il manque l'un d'eux est refusée nommément à l'enregistrement et au chargement de la configuration. Le slot et la publication sont créés quand la première table les suit et supprimés quand la dernière cesse de le faire ou que la source est supprimée. Au démarrage, un serveur vérifie chaque source PostgreSQL enregistrée et supprime un slot Provisa qu'aucune table native ne suit : un slot laissé par une configuration modifiée alors qu'aucun serveur ne tournait ne retient donc plus le WAL de la source.

Une table alimentée en push a besoin d'une clé primaire déclarée ; celle qui n'en a pas est ignorée et journalisée.

Une table dotée d'un signal de changement push est considérée comme fraîche sur le chemin de lecture, parce que son flux est son contrôle de fraîcheur. Un signal push que rien ne recevrait est donc refusé : `change_signal: native` sur tout type autre que Kafka, WebSocket, ingest, PostgreSQL, MongoDB ou Cosmos DB est refusé à l'enregistrement de la source ou de la table et au chargement de la configuration, de même que `debezium` sur une source sans bloc `cdc`.
