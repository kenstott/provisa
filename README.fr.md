# Provisa

**Connectez vos bases de données. Interrogez-les en 5 minutes en GraphQL, gRPC, SQL ou MCP, par n'importe quelle API ou protocole.**

Provisa met une seule API devant toutes vos données. Pointez-le vers vos bases de données et vos applications métier, décrivez comment elles s'articulent, et interrogez le résultat joint par REST, GraphQL, SQL, gRPC, MCP, JSON:API, Arrow Flight, pgwire ou Bolt.

Cela fonctionne parce que tout vient d'un modèle unique : vos domaines, les relations entre vos tables et les politiques qui s'y appliquent, sur toutes vos sources. Provisa s'exécute à partir de ce modèle et gouverne à partir de lui, il n'y a donc rien à synchroniser. Les relations enregistrées sont les seules jointures qu'une requête peut faire, et les politiques d'accès sont compilées dans chaque plan de requête. Quatre choses découlent de ce modèle unique :

- **Fédération.** 73 connecteurs atteignent plus de 85 types de sources, dont Salesforce, ServiceNow et Stripe. Les systèmes SaaS qui font tourner votre activité se lisent comme des tables à côté de vos bases de données, sur place, sans pipeline préalable vers un entrepôt. Interrogez-les en SQL, GraphQL ou Cypher, par neuf protocoles.
- **Métriques.** Une métrique est un agrégat nommé et gouverné. Vous la définissez une fois, et chaque outil et chaque langage qui la demande reçoit la même définition.
- **Ontologie.** Le glossaire métier est un graphe de concepts, de leurs définitions et de leurs liens. Provisa le rattache aux tables et colonnes qui contiennent les valeurs, ce qui lui permet de répondre à des questions sur vos données. Un concept reste un brouillon tant qu'il n'atteint pas des données, directement ou par d'autres concepts. Le glossaire dit ce qu'est un concept ; une métrique dit comment un nombre se calcule.
- **Gouvernance.** Six couches de contrôles sont compilées dans chaque plan de requête, dans chaque langage et par chaque protocole, et chaque requête est consignée de la même façon : qui a interrogé quoi, sous quel rôle, selon quelle politique. Les traces, métriques et journaux sont enregistrés comme tables interrogeables à côté de vos données métier.

Vous pouvez interroger en **GraphQL, Cypher ou SQL** et vous connecter par **pgwire, Bolt, gRPC, REST, Arrow Flight ou JDBC**. Chaque langage de requête est réduit à une seule représentation intermédiaire, et la gouvernance s'y applique, une fois, pour qu'une politique ne puisse pas diverger d'un langage à l'autre. À la sortie, cette forme est traduite dans le dialecte propre à chaque source. Un nouveau langage de requête est un nouveau frontal sur le même cœur.

Lectures et écritures y passent toutes deux. Une requête sur plusieurs sources se répartit par la couche de fédération. Les écritures, et les lectures sur une seule source, vont droit au pilote de cette source, gouvernées de la même façon et revenues en général en moins de 100 ms. Le streaming en colonnes d'Arrow Flight est intégré.

Le modèle a un vocabulaire réduit : domaines, relations, rôles et politiques. Il reste ainsi facile à lire et à auditer, et vous pouvez lire l'ensemble des politiques et savoir ce qu'il fait. Provisa lui-même est un compilateur de requêtes léger. Il ne se place jamais sur le chemin de vos données comme moteur d'exécution. Il transforme une requête en requêtes natives, les achemine et s'efface, et c'est pourquoi le patrimoine de données reste rapide.

Vous pouvez l'utiliser de deux manières, et elles se combinent :

- **Pour moderniser.** Modélisez votre patrimoine, laissez Provisa générer le SQL natif de chaque source, puis reprenez ce SQL dans le système cible. Provisa sert de passerelle pendant la transition, et vous pouvez vous en passer ensuite.
- **Pour gouverner durablement.** Gardez-le comme le chemin que prend chaque requête, pour que définition, application et audit restent ensemble tant que le patrimoine existe.

## Essayer une démo {: #try-a-demo }

Le plus rapide est une démo en direct dans le navigateur, sans rien installer : [provisa.dev/demos](https://provisa.dev/demos). Chacune ouvre un sandbox privé avec une visite guidée.

Depuis une copie de ce dépôt, vous pouvez exécuter n'importe quelle démo sur votre propre machine :

```bash
./start-ui-install.sh --list-demos     # the demos you can run, and which are published
./start-ui-install.sh --demo weblogs   # start one, with its tour
```

Une démo publiée est récupérée sous forme de bundle prêt à l'emploi ; les autres s'exécutent depuis `demo/named/<name>/`. Voir [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) pour toutes les options.

## Le modèle de fédération {: #the-federation-model }

Quatre idées portent toute la conception. Chaque source est ramenée à des tables. Chaque requête est ramenée à une forme proche de SQL. L'accessibilité décide de ce qui est interrogé en direct et de ce qui est d'abord copié. Et une stratégie de fraîcheur garde chaque copie, et chaque jeu de données dérivé, aussi à jour que nécessaire.

Commençons par les sources. Chacune s'exprime comme un ensemble de tables à deux dimensions sur un système de types commun, et c'est le seul contrat qu'une source doit remplir pour rejoindre le patrimoine. Une table MySQL ou PostgreSQL en est déjà une. Un résultat de requête GraphQL en devient une une fois aplati. Les triplestores SPARQL et Neo4j ne ressemblent en rien à des tables, mais vous fournissez une requête au résultat tabulaire, et la requête devient l'adaptateur. Quelle que soit la source, le patrimoine voit des lignes, des colonnes et des types. Intégrer un nouveau type de source, c'est remplir ce seul contrat, parfois avec un peu d'aide humaine. Personne n'écrit d'intégration sur mesure.

Les requêtes sont traitées de la même façon. SQL, avec tous ses dialectes, est la langue de l'analyse sur des tables, ce qui fait d'une forme proche de SQL la cible commune naturelle. Chaque requête, dans quelque langage qu'elle arrive, est donc d'abord ramenée à cette forme. SQL et GraphQL s'y ramènent facilement. Les chemins et la sémantique de graphe de Cypher demandent un vrai travail, mais ils y arrivent. Comme tout passe par une seule forme avant toute autre chose, la gouvernance s'applique en un seul endroit, quel que soit le langage de la requête.

Avec des sources tabulaires et une seule forme de requête, la fédération peut couvrir à la fois l'interrogation en direct et l'entreposage : ce que fait un moteur de requêtes en direct comme Trino, plus la matérialisation sur laquelle ces moteurs s'appuient. L'**accessibilité** relie les deux. Le moteur peut-il interroger une source là où elle est, ou faut-il d'abord copier ses données dans un endroit interrogeable ? La réponse partage le patrimoine entre ce qui est lu en direct et ce qui est copié.

La plupart des bases de données ont déjà une forme de lien direct : `ATTACH` dans DuckDB, `postgres_fdw` dans PostgreSQL, les liens externes de Databricks. La plupart peuvent donc servir de moteur de fédération jusqu'à un certain point. Aucune ne couvre tout. Chacune atteint un ensemble précis de sources et copie le reste, et aucune ne dit clairement ce qui relève de quoi. Le modèle rend l'accessibilité explicite, sous la forme d'un ensemble défini de méthodes par source qui disent ce que le moteur atteint en direct et, par élimination, ce qui doit être copié.

Reste la fraîcheur. Pour chaque source que le moteur n'atteint pas, à quel point sa copie doit-elle être à jour ? En pratique, les réponses sont peu nombreuses : à la demande, selon un calendrier, sur un signal de changement (CDC, un filigrane, un instantané) ou figée. En choisir une par source, c'est toute la politique de fraîcheur.

Les jeux de données dérivés entrent dans la même forme : tables dérivées, agrégats, sortie d'une transformation. Ils s'écrivent dans la même forme, si bien que le lignage vient tout seul. Le chemin de chaque système d'origine jusqu'à une sortie finale est la définition qui l'a produite, lisible de bout en bout. Les construire pose la question de la fraîcheur un cran plus loin : le jeu de données se rafraîchit-il selon un calendrier, seulement quand ses préconditions sont remplies, en continu, ou comme un instantané historique figé ? Les choix sont le même petit ensemble que pour la copie d'une source, et la politique de construction d'un jeu dérivé emploie donc le même vocabulaire.

Les modèles dimensionnels en découlent directement. Les faits et les dimensions d'un schéma en étoile sont des jeux dérivés comme les autres. Une dimension est une projection conforme et dédupliquée, une table de faits est une jointure avec agrégat ramenée à un grain, chacune avec sa propre politique de construction et de fraîcheur. Les dimensions à évolution lente n'ont besoin de rien de particulier : un instantané figé est un historique de type 2, une reconstruction planifiée est de type 1. Comme le schéma est défini dans le modèle et n'est lié aux tables d'aucun entrepôt, les mêmes faits et dimensions peuvent être construits dans Oracle, dans Databricks, ou rester virtuels au-dessus d'un moteur MPP, sans remodélisation.

Data Vault s'y insère de la même façon, une couche plus tôt. Les hubs sont des jeux dédupliqués de clés métier, les links sont les relations enregistrées entre eux, et les satellites sont des jeux d'attributs en insertion seule, horodatés, qui gardent l'historique. Un satellite n'est qu'un jeu dérivé suivant la stratégie du signal de changement : date de chargement plus hashdiff, c'est la capture de changements appliquée à des attributs descriptifs, et l'historique en insertion seule, c'est la stratégie de l'instantané figé. Les tables point-in-time et bridge sont d'autres jeux dérivés, construits pour la vitesse des requêtes. Un raw vault est donc un ensemble de jeux dérivés dans le modèle, et un schéma en étoile en est une projection, tous deux générés et tous deux portables d'un moteur à l'autre. La méthode reste votre décision : ce qui devient un hub, le grain d'un satellite, la façon de découper. Une fois décidé, la décision vit dans le modèle. Aucun ETL ne la soude à un entrepôt.

Vous déclarez ces deux modèles avec **deux raccourcis** plutôt qu'avec des vues écrites à la main. Ce sont les briques dont sont faits tous les schémas en étoile et tous les Data Vault, et elles ne vous engagent dans aucune des deux méthodes :

- **`entity`** est une projection d'une source, avec clé et dédupliquée, qui peut garder l'historique. Déclarez la clé, les attributs et un mode d'historique, et Provisa construit une vue matérialisée. Si vous demandez de l'historique, il construit à la place une **MV bitemporelle** (`scd2` → delta, `snapshot` → snapshot). Une seule construction vous donne une **dimension** Kimball (SCD1 ou SCD2) ou un **hub avec satellite** Data Vault.
- **`fact`** se joint à des clés d'entité, se ramène à un grain déclaré et agrège des mesures. Provisa construit une MV d'agrégat plus les relations enregistrées vers les entités. Une seule construction vous donne une **table de faits** en étoile ou un **link** Data Vault ; un fait sans mesure est un link d'ensemble de clés pur.

Chaque `entity` ou `fact` devient exactement les vues matérialisées, réglages bitemporels et relations qu'un modélisateur écrirait sinon à la main : l'entrepôt reste dans le modèle et passe d'un moteur à l'autre sans remodélisation. Déclarez-en un dans l'interface d'administration (le formulaire **Model**) ou par l'API d'administration (`registerEntity` / `registerFact`). Le modèle vous génère une étoile Kimball ou un Data Vault ; il n'impose ni l'un ni l'autre.

### Voyage dans le temps {: #time-travel }

Voyager dans le temps, c'est garder chaque version d'une ligne au lieu de l'écraser, pour pouvoir demander ce qu'étaient les données à n'importe quel moment passé. Les moteurs diffèrent beaucoup dans l'efficacité avec laquelle ils le font ; Provisa en fait donc une propriété de la **définition** de la vue matérialisée, quel que soit le moteur de stockage. Déclarez-le une fois et il fonctionne sur tout stockage capable d'héberger une vue matérialisée.

Ce qui le rend portable, c'est qu'il est **en ajout seul**. Une version écrite n'est jamais modifiée ni supprimée. L'astuce bitemporelle habituelle qui consiste à clôturer une ligne en réécrivant une date « valide jusqu'à » demande un UPDATE, que beaucoup de moteurs ne savent pas faire à bon compte, voire pas du tout, sur un stockage fédéré. Chaque rafraîchissement **ajoute** donc, et la version en vigueur à un instant donné est déterminée à la lecture, à partir du journal. Il y a deux façons d'ajouter :

- **Snapshot** ajoute tout le jeu de données frais, horodaté avec l'heure système du rafraîchissement. Pas de calcul de différences, et c'est correct sur tous les moteurs, mais le stockage grossit d'une copie complète par rafraîchissement.
- **Delta** n'ajoute que ce qui a changé, plus des pierres tombales pour les clés supprimées. Le moteur calcule lui-même le delta, avec des anti-jointures dans un `INSERT … SELECT`. Provisa ne l'assemble jamais ligne par ligne. C'est plus petit, et il faut une clé d'entité.

Provisa gère ainsi le temps système, c'est-à-dire le moment où il a enregistré une version. Le temps de validité, le moment où un fait est vrai pour le métier, vient du SELECT de la vue et est conservé. Les moteurs qui font mieux, comme les instantanés Iceberg natifs ou un MERGE qui touche moins de lignes, peuvent être utilisés derrière la même déclaration ; le chemin en ajout seul est le plancher, correct partout.

La lecture ne demande aucune syntaxe particulière. Une requête simple sur une MV bitemporelle renvoie l'état **actuel**, reconstruit à partir du journal. Pour remonter le temps, envoyez un en-tête `X-Provisa-As-Of: <timestamp>` et toute la requête reçoit la réponse telle qu'était le patrimoine à ce moment-là, de la même façon sur tout stockage. Ajoutez `X-Provisa-As-Recorded` pour lire les données telles que Provisa les avait enregistrées à un instant, afin qu'une correction enregistrée plus tard ne change pas la relecture du passé. Activez-le pour toute vue matérialisée dans l'interface d'administration (la commande **Time Travel** : désactivé, snapshot ou delta, plus une clé d'entité) ou par l'API d'administration.

L'accessibilité et la fraîcheur forment ensemble un modèle général de fédération : une définition de ce qui est en direct, de ce qui est copié et de la fraîcheur de chaque copie, indépendante de ce qu'un moteur atteint. Le modèle est portable, si bien que votre patrimoine n'est pas lié au fournisseur dont la fédération atteint aujourd'hui le plus de sources.

## Fonctionnalités {: #features }

### Interfaces de requête {: #query-interfaces }

Ce sont les langages et les API structurées dans lesquels vous écrivez vos requêtes. Chacun a sa syntaxe et ses sémantiques ; la gouvernance (RLS, masquage, visibilité des colonnes, application des relations) s'applique uniformément à tous, quel que soit le protocole de transport qui les achemine.

- **GraphQL** — Schémas par rôle avec visibilité par champ, filtrage, pagination par curseur et requêtes d'agrégat (`count`, `sum`, `avg`, `min`, `max`). Le schéma ne propose que les relations enregistrées ; chaque requête est donc structurellement valide par construction, et GraphQL est le chemin le plus rapide vers une requête simple correcte. Apollo APQ est inclus : les requêtes sont hachées et enregistrées sur le serveur, les appels suivants n'envoient que le hachage en HTTP GET, et les réponses deviennent cachables par un CDN sans modifier le client. Les tables de référence sous un seuil de lignes configurable apparaissent comme types enum.
- **SQL** — SQL complet sur les données fédérées, sans contrainte et plus expressif que GraphQL. Écrivez du SQL standard, sous-requêtes corrélées comprises, et il s'exécute tel quel à travers les sources. Une requête sur une seule source saute entièrement la couche de fédération (moins de 100 ms).
- **Cypher** — Langage de requête de graphe sur le même schéma fédéré. Parcourez les relations comme des arêtes de graphe ; unissez les sources ; chemins de longueur variable. La gouvernance s'applique à l'identique de GraphQL et SQL.
- **API de modèle gRPC** — Un `.proto` généré à partir du schéma enregistré, avec des RPC typées de requête et d'insertion pour chaque table à la racine du schéma d'un rôle, et des réponses en flux. Comme GraphQL, elle est pilotée par le schéma : le modèle d'enregistrement est le contrat, et protobuf est l'encodage réseau. C'est une interface de requête complète par table, là où Arrow Flight est un transport de flux en colonnes.
- **JSON:API** — Une API de requête structurée à `/data/jsonapi/{table}`, en HTTP uniquement par conception. Elle prend en charge JSON:API 1.1 : jeux de champs partiels (`fields[table]=col1,col2`), expressions de filtre (`filter[field][op]=value`), documents composés (`include=relation`) et tri. Elle interroge une table à la fois avec une syntaxe de filtre standard ; utilisez SQL ou GraphQL quand il vous faut un langage de requête généraliste.
- **Explorateur de langages de requête** — Écrivez une requête GraphQL et voyez ses traductions **Semantic SQL** et **Cypher** en direct dans des panneaux latéraux ; copiez l'une ou l'autre ou passez directement dans l'éditeur SQL ou Graphe. Un flux de travail pratique consiste à esquisser des fragments de requête en GraphQL, puis à assembler le SQL obtenu en vues ou rapports complexes.

L'explorateur montre une requête GraphQL à côté de ses traductions SQL et Cypher en direct :

![Explorateur de langages de requête](docs/images/query-explorer.png)

Le même schéma fédéré s'affiche aussi sous forme de graphe vivant, avec les étiquettes de domaine et de nœud, les types de relation et les parcours de longueur variable :

![Visualisation du graphe](docs/images/graph-view.png)

### Outils de composition de requêtes {: #query-composition-tools }

Ces outils vous aident à écrire des requêtes dans les langages ci-dessus. Ce ne sont pas eux-mêmes des langages de requête.

- **Requête en langage naturel** — Un pipeline NL→SQL/Cypher/GraphQL qui s'appuie sur Claude. Décrivez ce que vous voulez en langage courant, et le pipeline écrit une requête dans le langage choisi, avec une boucle de validation interactive avant toute exécution.

![Requête en langage naturel](docs/images/natural-language.png)

### Protocoles de transport {: #wire-protocols }

Voici les protocoles de connexion. SQL, GraphQL et Cypher passent par eux, et le protocole choisi ne change ni l'interface de requête ni le comportement de la gouvernance.

- **pgwire** — N'importe quel client PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, `read_sql` de pandas) se connecte sur le port 5439 comme s'il s'agissait d'un serveur Postgres. N'accepte que du SQL. Le pipeline de gouvernance complet s'applique. `pg_catalog` et `information_schema` sont répondus depuis un catalogue en mémoire, si bien que les explorateurs de schéma fonctionnent sans aller-retour vers la fédération. TLS facultatif.
- **Bolt (Neo4j)** — N'importe quel client Neo4j (Neo4j Browser, Bloom, pilotes officiels) se connecte via le protocole Bolt et exécute du Cypher contre le graphe fédéré. Chaque rôle que détient l'utilisateur apparaît comme une base `provisa_<role>`. Même gouvernance que sur tout autre transport. TLS facultatif.
- **Arrow Flight** — Streaming colonnaire à haut débit sur gRPC ; accepte GraphQL ou SQL comme entrée de requête. Jeux de résultats non bornés, aucune matérialisation côté serveur, aucune infrastructure distincte requise.
- **JDBC** — Intégration aux outils de BI (Tableau, Power BI, DBeaver).
- **WebSocket / SSE** — Abonnements : événements de changement en quasi temps réel ; backends : PG natif, MongoDB natif, CDC, interrogation périodique. Également exposés via Kafka.

### Sources de données {: #data-sources }

- **73 connecteurs, plus de 85 types de source** — Le plus grand nombre compte séparément les services PostgreSQL hébergés (RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon) et les formats de fichier du File Crawler. PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestores SPARQL, Kafka, Google Sheets, Kaggle, Salesforce, ServiceNow, Stripe et les autres se trouvent derrière une seule API. Les sources graphe et RDF sont à égalité avec les sources relationnelles.
- **Routage intelligent** — Une requête sur une seule source saute la fédération (moins de 100 ms). Une requête multi-sources passe par la couche de fédération, sur votre propre cluster ou sur les workers embarqués.
- **Sources API** — Enregistrez des points de terminaison REST, GraphQL, gRPC, WebSocket ou RSS comme tables interrogeables, avec des assistants SPARQL inclus. Les jointures fédérées entre sources API et sources relationnelles ne demandent aucune configuration supplémentaire.
- **Introspection de schémas distants** — Pointez vers n'importe quel endpoint GraphQL, OpenAPI ou gRPC ; les opérations documentées sont automatiquement exposées comme tables interrogeables, nœuds et arêtes de graphe, avec toute la gouvernance appliquée par-dessus
- **Sources fichiers** — Fichiers CSV, Parquet et SQLite comme tables interrogeables ; prend en charge les chemins locaux et le stockage objet distant (`s3://`, `ftp://`, `sftp://`)
- **Jeux de données Kaggle** — recherchez le catalogue public de Kaggle et enregistrez un jeu de données par recherche en direct authentifiée par jeton, sans téléchargement manuel ; s'enregistre comme une source de fichiers unique dont les tables sont découvertes automatiquement, uniquement pour les paquets CSV/Parquet
- **Intégration Kafka** — Topics comme tables en lecture seule ; résultats de requêtes comme receveurs Kafka
- **Déclencheurs planifiés** — Déclencheurs cron et par intervalle (APScheduler) qui appellent des webhooks, des mutations ou des publications vers un receveur Kafka
- **Indications de fédération** — Des indications de routage en commentaire SQL surchargent les décisions de routage automatiques

![Sources de données](docs/images/data-sources.png)

Sources, fichiers et endpoints distants sont enregistrés comme tables gouvernées depuis l'interface :

![Enregistrement de table](docs/images/table-registration.png)

### Sécurité et gouvernance {: #security-governance }

- **Sécurité au niveau des lignes** — Injection de clause WHERE par table et par rôle
- **Masquage de colonnes** — Masquage par colonne (expression régulière, constante, troncature) avec contournement selon le rôle
- **Valeurs prédéfinies de colonnes** — Valeurs statiques ou de variable de session injectées côté serveur à l'insertion et à la mise à jour ; non exposées dans les types d'entrée des mutations
- **Permissions d'écriture** — Contrôle d'accès aux mutations par colonne (`writable_by`)
- **Rôles hérités** — Les rôles héritent récursivement du RLS, de la visibilité et du masquage d'un rôle parent
- **Fonctions et webhooks suivis** — Fonctions de base de données et webhooks sortants exposés comme mutations GraphQL avec des formes de retour typées
- **Point d'ancrage d'approbation ABAC** — Point d'ancrage d'autorisation avant exécution ; transport webhook, gRPC ou unix_socket ; portée par table, par source ou globale ; politique de repli configurable
- **Authentification enfichable** — Firebase, Keycloak, OAuth 2.0, simple (pour les tests)

![Rôles de sécurité](docs/images/security-roles.png)

### Livraison et performance {: #delivery-performance }

- **Vues matérialisées comme transformations enregistrées** — Une MV enregistre la transformation qui l'a produite : sa forme de jointure ou son SQL, les signaux d'entrée par source à partir desquels elle a été construite (snapshot Iceberg, watermark RDB) et un contrôle de déterminisme à l'enregistrement. Comme la transformation est enregistrée, Provisa peut réécrire une requête, ou une partie, sur une MV fraîche sans que vous changiez la requête. La correspondance porte sur les motifs de jointure et accepte les correspondances partielles ; une MV qui couvre une partie des jointures s'applique donc quand même, et les jointures restantes sont conservées.
- **Inlining des tables chaudes** — Les petites tables de correspondance fréquemment jointes sont insérées comme CTE VALUES directement dans le plan de requête, éliminant les allers-retours inter-sources pour les données dimensionnelles
- **Cache de requêtes** — Cache de résultats Redis partitionné par rôle et RLS ; cache de hachages APQ inclus
- **L'observabilité comme donnée** — Les traces distribuées, métriques et journaux sont collectés par OpenTelemetry, compactés dans Iceberg sur S3 et enregistrés comme tables interrogeables (`traces`, `metrics`, `logs`, `queries`) dans le schéma fédéré. Interrogez-les en SQL, GraphQL ou Cypher à côté de vos données métier. Joignez par exemple une table `customers` à `queries` pour voir qui a lancé quoi et en combien de temps.

### Administration et intégration {: #administration-integration }

- **API d'administration** — GraphQL à `/admin/graphql` ; téléversement/téléchargement de configuration, édition des relations, approbation des requêtes
- **Visionneuse de rapports** — `/admin/reports` liste les vues de gestion intégrées du domaine ops ainsi que tout rapport personnalisé enregistré ; exige la capacité `observability`
- **Aperçu de table** — chaque table enregistrée dispose d'une visionneuse de données gouvernée, paginée côté serveur, avec filtres poussés vers la source, regroupement multi-niveaux et export CSV
- **GraphQL Voyager** — Visualisation interactive du schéma, cadrée par rôle, sous forme de diagramme entité-relation
- **Découverte de relations par LLM** — Claude suggère des candidats de clés étrangères.
- **Client Python** — `pip install provisa-client` ; GraphQL/SQL → DataFrames, Arrow Flight → tables pyarrow, dialecte SQLAlchemy, prise en charge d'ADBC
- **Ingestion de données** — Endpoints HTTP pour pousser des données d'événements JSON dans la plateforme
- **Import Hasura v2 / DDN** — Convertit des métadonnées Hasura v2 ou un YAML de supergraphe DDN en configuration Provisa
- **Apollo Federation** — Expose Provisa comme sous-graphe Apollo Federation v2

Schéma cadré par rôle, visualisé comme diagramme entité-relation (GraphQL Voyager) :

![Voyager du schéma](docs/images/schema-voyager.png)

Les relations sont enregistrées, approuvées et appliquées comme les seuls chemins de JOIN licites :

![Relations](docs/images/relationships.png)

## Modèle de sécurité {: #security-model }

C'est ici que « sur le chemin que prend déjà chaque requête » doit tenir. Provisa applique un même modèle de sécurité en couches à chaque langage de requête (GraphQL, SQL, Cypher) et à chaque transport (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket), et aucun chemin de requête ne le contourne. La couverture vient de la conception, donc personne n'a rien à retenir : ajoutez une source, une colonne ou une relation, et chaque couche s'y applique aussitôt, sans rien de plus à enregistrer.

Les couches s'appliquent dans l'ordre, et une requête doit franchir chacune avant que la suivante soit vérifiée.

### Couche 0 — Filtrage de l'introspection {: #layer-0-introspection-filtering }

Le schéma et le catalogue qu'un rôle voit ne contiennent que les tables de sa liste `domain_access` et les colonnes qui passent leurs règles `visible_to`. Tout ce qui sort de l'accès du rôle est invisible d'emblée : impossible de l'interroger, de l'autocompléter, ni même d'en déduire l'existence. Cela couvre le schéma GraphQL, le catalogue SQL et le navigateur de schéma de l'éditeur de requêtes.

### Couche 1 — Accès public {: #layer-1-public-access }

Les tables des domaines sans restriction `domain_access` sont visibles par toute identité authentifiée, sans rien de plus à configurer. Les données vraiment publiques ne demandent aucun effort.

### Couche 2 — Accès au domaine {: #layer-2-domain-access }

Chaque rôle porte une liste `domain_access` d'ID de domaines, et une requête qui touche une table hors de ces domaines est refusée avant de s'exécuter. C'est la grande frontière de propriété : un rôle RH n'atteint pas les tables financières, quelle que soit l'écriture du SQL.

### Couche 3 — Sécurité au niveau des lignes {: #layer-3-row-level-security }

Une fois l'accès au domaine confirmé, des prédicats `WHERE` par table et par rôle sont ajoutés à chaque `SELECT` à l'exécution, et évalués sur les données brutes. Une responsable régionale qui interroge une table de commandes partagée ne voit que les lignes de sa région, même avec `SELECT *`.

### Couche 4 — Visibilité et masquage des colonnes {: #layer-4-column-visibility-and-masking }

Les colonnes dont la liste `visible_to` exclut le rôle demandeur sont retirées de la sortie. Les colonnes soumises à une règle de masquage voient leurs valeurs remplacées par un caviardage regex, une constante ou une troncature avant que les résultats ne quittent le serveur. Le masquage s'applique dans chaque langage de requête et chaque format de sortie.

### Couche 5 — Garde des prédicats {: #layer-5-predicate-guard }

Les colonnes masquées sont refusées dans les clauses `WHERE` et `HAVING`. Sinon, un appelant pourrait retrouver la vraie valeur par recherche dichotomique dans un filtre, même avec une sortie masquée. Le refus a lieu à l'analyse de la requête, avant toute exécution.

### Gouvernance des relations {: #relationship-governance }

En SQL, une condition JOIN doit correspondre à une relation enregistrée et approuvée entre les tables, et une jointure non approuvée est refusée. Chaque relation porte une raison et une description lisibles, qui disent aux personnes comme aux agents autonomes pourquoi ce chemin existe. C'est une politique de gouvernance et non une frontière de sécurité stricte. Les couches 2 à 5 tiennent quelle que soit la forme de la jointure, donc un contournement délibéré n'expose rien que le rôle ne pourrait atteindre avec deux requêtes séparées, et chaque tentative est journalisée pour l'audit.

Aucune jointure invalide ne s'exécute jamais sur vos données. Les rôles sont gouvernés par défaut, et en sortir veut dire accorder délibérément le droit `ignore_relationships` à un rôle de confiance. Le jeton OAuth d'un agent correspond à un rôle, donc un agent reçoit les mêmes couches qu'une personne. Les catalogues de données dotés d'une couche de contexte disent à un agent ce qu'il devrait faire ; Provisa impose ce qu'il peut faire.

---

Ces couches se combinent. Un rôle avec accès aux domaines, RLS et colonnes masquées a toutes les couches actives à la fois, car chaque couche se configure séparément et s'applique à toute requête qui touche un objet gouverné.

## Démarrage rapide {: #quick-start }

### macOS

1. Téléchargez [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (toujours la dernière version)
2. Faites glisser **Provisa.app** dans `/Applications` et double-cliquez pour lancer
3. Le premier lancement effectue une configuration unique (environ 2 min, sans besoin d'Internet)
4. Ouvrez le Terminal :

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Téléchargez [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (toujours la dernière version)
2. Rendez-le exécutable et lancez-le. Le premier lancement effectue une configuration unique (sans besoin d'Internet) :

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Téléchargez [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (toujours la dernière version)
2. Lancez l'installateur. Aucun droit d'administrateur requis
3. Ouvrez **Provisa First Launch** depuis le menu Démarrer. Effectue une configuration unique (environ 5 min, sans besoin d'Internet)
4. Ouvrez un nouveau terminal :

```bash
provisa start
```

### Première requête {: #first-query }

En développement local (`PROVISA_MODE=test`), aucun identifiant n'est requis. En production, vous vous authentifiez avec un jeton Bearer, et Provisa en lit le rôle.

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

Téléchargez [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (toujours la dernière version) et ajoutez-le au chemin des pilotes de votre outil de BI.

```text
jdbc:provisa://localhost:8815
```

Authentifiez-vous avec votre nom d'utilisateur et votre mot de passe Provisa, et le serveur vous attribue votre rôle.

Voir [docs/integrations.md](docs/integrations.md) pour les étapes de configuration de Tableau et Power BI.

### Protocole de transport PostgreSQL (pgwire) {: #postgresql-wire-protocol-pgwire }

Provisa parle le protocole réseau de PostgreSQL sur le port 5439. Tout client capable de se connecter à Postgres se connecte à Provisa, sans pilote, sans adaptateur et sans modifier vos outils existants.

**Le nom d'utilisateur PostgreSQL sélectionne le rôle Provisa.** Avec `provider: none` (mode confiance), le mot de passe est ignoré et tout nom de rôle configuré est accepté comme nom d'utilisateur — connectez-vous en `analyst`, `admin` ou tout autre rôle pour voir la vue gouvernée des données propre à ce rôle. Avec `provider: simple`, le mot de passe est validé par bcrypt. Les autres fournisseurs (`firebase`, `keycloak`, `oauth`) ne sont pas pris en charge sur pgwire.

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

Chaque requête passe par le pipeline de gouvernance complet : accès aux domaines, RLS, masquage et garde de prédicats s'appliquent exactement comme pour GraphQL et REST. Les navigateurs de schéma (DBeaver, DataGrip, pgAdmin) fonctionnent sans configuration. Provisa répond à leurs requêtes `pg_catalog` et `information_schema` depuis un catalogue en mémoire limité à l'accès aux domaines du rôle ; les utilisateurs ne voient donc que les tables et colonnes qu'ils peuvent interroger.

DataGrip parcourt le schéma gouverné et son diagramme de clés étrangères via pgwire, sans pilote ni adaptateur :

![Provisa dans DataGrip via pgwire](docs/images/pgwire-datagrip.png)

Le TLS s'active en définissant `PROVISA_PGWIRE_CERT` et `PROVISA_PGWIRE_KEY`. Le port est configurable via `PROVISA_PGWIRE_PORT` (`5439` par défaut).

### Bolt (protocole de transport Neo4j) {: #bolt-neo4j-wire-protocol }

Provisa parle aussi le protocole **Bolt** de Neo4j : les outils natifs de graphe se connectent directement et exécutent du Cypher sur le graphe fédéré. Rien n'est exporté, et aucune base de graphe séparée n'est nécessaire. Pointez **Neo4j Browser** ou **Bloom** vers Provisa et parcourez les relations entre sources sous la même gouvernance (accès aux domaines, RLS, masquage).

Neo4j Browser exécutant du Cypher contre Provisa. Étiquettes de nœuds, types de relations et clés de propriétés viennent directement du schéma enregistré :

![Provisa dans Neo4j Browser via Bolt](docs/images/bolt-neo4j-browser.png)

Activez-le en définissant `PROVISA_BOLT_PORT` (Neo4j utilise `7687` par défaut), et activez TLS avec `PROVISA_BOLT_CERT` et `PROVISA_BOLT_KEY`. Chaque rôle Provisa détenu par l'utilisateur authentifié apparaît comme une base de données sélectionnable `provisa_<role>` (le sélecteur `provisa_admin` ci-dessus). En choisir une restreint la session aux droits de domaine de ce rôle, et l'utilisateur ne peut jamais dépasser les rôles qu'il détient.

### Client Python {: #python-client }

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role URL parameter
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Voir [docs/python-client.md](docs/python-client.md) pour la référence complète.

## Documentation

| Sujet | Document |
| --- | --- |
| Démarrage rapide développeur (exécution depuis les sources) | [docs/quickstart.md](docs/quickstart.md) |
| Référence complète de la configuration YAML | [docs/configuration.md](docs/configuration.md) |
| Référence des endpoints (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Conception du système et carte des composants | [docs/architecture.md](docs/architecture.md) |
| Modèle de sécurité (RLS, masquage, authentification) | [docs/security.md](docs/security.md) |
| Stockage des secrets et références `${secret:NAME}` | [docs/secrets.md](docs/secrets.md) |
| Glossaire métier et curation des termes | [docs/glossary.md](docs/glossary.md) |
| Environnements (dev / staging / prod) | [docs/environments.md](docs/environments.md) |
| Construire des environnements de test (profilage, données factices, données synthétiques) | [docs/test-data.md](docs/test-data.md) |
| Référence des méthodes de données factices | [docs/fake-methods.md](docs/fake-methods.md) |
| Exécuter et construire des démos sur un portable (ingénieurs avant-vente) | [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) |
| Types de sources pris en charge | [docs/sources.md](docs/sources.md) |
| Abonnements SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, outils de BI, clients Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Client Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| API d'administration | [docs/admin.md](docs/admin.md) |
| Déploiement (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Import Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Flux de publication (étiquettes alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionnement {: #sizing }

Provisa embarque un moteur de fédération pour les requêtes multi-sources. Au premier lancement vous choisissez un budget de RAM ; Provisa en dérive automatiquement le nombre de workers de fédération locaux.

| RAM de l'hôte | Workers | Charge de travail typique |
| --- | --- | --- |
| < 24 Go | 0 | Développement, requêtes mono-source, petites équipes |
| 24–47 Go | 1 | Petite équipe, requêtes inter-sources modérées |
| 48–95 Go | 2 | Déploiement départemental, usage mixte BI + carnets |
| 96 Go et plus | 4 | Grand département, fédération concurrente intensive |

Le nombre de workers peut être changé à tout moment en modifiant `~/.provisa/config.yaml` (`federation_workers: N`) puis en exécutant `provisa restart`. Mettez-le à `0` pour n'exécuter que la coordination (nœud unique).

### Passer à l'échelle au-delà d'une seule machine {: #scaling-beyond-a-single-box }

**Montée en charge horizontale** — Faites tourner plusieurs instances Provisa derrière un répartiteur de charge. Chaque instance est un système complet. Toutes doivent pointer vers la même base de configuration (définissez `CONFIG_DB_HOST` sur les machines secondaires), et elles peuvent partager une instance Redis (`REDIS_URL`) pour un cache unique. La plupart des requêtes se répartissent sans changement. Une très grande jointure entre sources peut dépasser une instance ; il faut alors une machine plus grosse ou un cluster de fédération externe.

**Redis partagé** — Définissez `REDIS_URL` sur chaque instance pour pointer vers un Redis externe. Un Redis partagé signifie que les entrées de cache d'une instance sont disponibles pour toutes, améliorant le taux de succès à l'échelle du cluster.

**Apportez votre propre cluster de fédération** — Pointez Provisa vers un cluster de fédération externe existant plutôt que vers les workers embarqués. Recommandé pour les déploiements à grande échelle ou en cloud ; voir [docs/deployment.md](docs/deployment.md) pour la configuration.

## Licence {: #license }

Business Source License 1.1 (non modifiée, selon les engagements du concédant de
MariaDB). Chaque version publiée bascule vers la Change License (GPL v2.0 ou
ultérieure) au 4e anniversaire de sa publication ; le code actuel et récent reste
sous BSL. Un usage en production au-delà des seuils de l'Additional Use Grant
(moins de 100 employés/sous-traitants et moins de 1 M$ de chiffre d'affaires sur
l'année précédente) exige une licence commerciale. Voir [LICENSE](LICENSE).

Le concédant ne consent pas à l'usage de cette œuvre pour l'entraînement d'IA ou
de ML. Voir [NOTICE](NOTICE), [ai.txt](ai.txt) et [robots.txt](robots.txt). Pour
les licences commerciales ou d'entraînement d'IA : <kennethstott@gmail.com>
